package main

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Layout of a configuration without [feeds.*] (one anonymous feed):
//   /tick/<SYMBOL>/<YYYY-MM-DD>.parquet       tick-to-tick best bid and ask, and trades, for that symbol and day
//   /orderbook/<SYMBOL>/<YYYY-MM-DD>.parquet  the top orderbook.depth levels per side (config, default 5), a row whenever one changes, and trades
//   /stream/<SYMBOL>/<YYYY-MM-DD>.parquet     every order message (add, execute, cancel, delete, replace, trade, cross), no book
//   .../<YYYY-MM-DD>.csv                      the same as CSV
//   /.status                                  JSON: running and pending extractions, failures
// With [feeds.NAME] tables every feed has a directory of its own holding the same tree: /<feed>/tick/<SYMBOL>/<day>.parquet and so on.
// Each kind directory lists the symbols of the newest day.
// Opening a file that is not cached yet triggers the extraction (Store.Request).

type rootNode struct {
	fs.Inode
	store   *Store
	started time.Time
}

// feedNode is /<feed>: the kinds of one feed. Without named feeds the root plays this part itself.
type feedNode struct {
	fs.Inode
	store   *Store
	feed    string
	started time.Time
}

type kindNode struct {
	fs.Inode
	store   *Store
	feed    string
	kind    string
	started time.Time
}

type symbolNode struct {
	fs.Inode
	store   *Store
	feed    string
	kind    string
	symbol  string
	started time.Time
}

type fileNode struct {
	fs.Inode
	store     *Store
	feed      string
	kind      string
	symbol    string
	day       string
	extension string
	started   time.Time
}

type statusNode struct {
	fs.Inode
	store   *Store
	started time.Time
}

var (
	_ fs.NodeLookuper  = (*rootNode)(nil)
	_ fs.NodeReaddirer = (*rootNode)(nil)
	_ fs.NodeGetattrer = (*rootNode)(nil)
	_ fs.NodeLookuper  = (*feedNode)(nil)
	_ fs.NodeReaddirer = (*feedNode)(nil)
	_ fs.NodeGetattrer = (*feedNode)(nil)
	_ fs.NodeLookuper  = (*kindNode)(nil)
	_ fs.NodeReaddirer = (*kindNode)(nil)
	_ fs.NodeGetattrer = (*kindNode)(nil)
	_ fs.NodeLookuper  = (*symbolNode)(nil)
	_ fs.NodeReaddirer = (*symbolNode)(nil)
	_ fs.NodeGetattrer = (*symbolNode)(nil)
	_ fs.NodeGetattrer = (*fileNode)(nil)
	_ fs.NodeOpener    = (*fileNode)(nil)
	_ fs.NodeGetattrer = (*statusNode)(nil)
	_ fs.NodeOpener    = (*statusNode)(nil)
)

// stableIno derives an inode number from the path, so the same file keeps the same inode across lookups
// (cp and similar tools treat a changing inode as "file replaced while being copied").
func stableIno(path string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(path))
	return hash.Sum64() | 1<<32 // never collides with the small numbers go-fuse hands out itself
}

// NewRoot returns the root node of the filesystem.
func NewRoot(store *Store) fs.InodeEmbedder {
	return &rootNode{store: store, started: time.Now()}
}

func directoryAttributes(out *fuse.Attr, started time.Time) {
	out.Mode = syscall.S_IFDIR | 0o555
	out.Nlink = 2
	out.SetTimes(&started, &started, &started)
}

func fileAttributes(out *fuse.Attr, size int64, modified time.Time) {
	out.Mode = syscall.S_IFREG | 0o444
	out.Nlink = 1
	out.Size = uint64(size)
	out.SetTimes(&modified, &modified, &modified)
}

func noCache(out *fuse.EntryOut) {
	out.SetEntryTimeout(0) // sizes and existence change as extraction completes: never trust the kernel cache
	out.SetAttrTimeout(0)
}

func (node *rootNode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	directoryAttributes(&out.Attr, node.started)
	out.SetTimeout(0)
	return 0
}

func (node *rootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{{Name: ".status", Mode: syscall.S_IFREG}}
	if node.store.config.Named() {
		for _, feed := range node.store.config.Feeds {
			entries = append(entries, fuse.DirEntry{Name: feed.Name, Mode: syscall.S_IFDIR})
		}
	} else {
		entries = append(entries, kindEntries()...)
	}
	return fs.NewListDirStream(entries), 0
}

func kindEntries() []fuse.DirEntry {
	entries := make([]fuse.DirEntry, 0, len(kinds))
	for _, kind := range kinds {
		entries = append(entries, fuse.DirEntry{Name: kind, Mode: syscall.S_IFDIR})
	}
	return entries
}

// lookupKind makes the node of /<feed>/<kind> (or /<kind> without named feeds).
func lookupKind(ctx context.Context, parent fs.InodeEmbedder, store *Store, feed, name string, started time.Time, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if !validKind(name) {
		return nil, syscall.ENOENT
	}
	directoryAttributes(&out.Attr, started)
	return parent.EmbeddedInode().NewInode(ctx, &kindNode{store: store, feed: feed, kind: name, started: started}, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno("/" + joinFeed(feed, name, "/"))}), 0
}

func (node *feedNode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	directoryAttributes(&out.Attr, node.started)
	out.SetTimeout(0)
	return 0
}

func (node *feedNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return fs.NewListDirStream(kindEntries()), 0
}

func (node *feedNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	noCache(out)
	return lookupKind(ctx, node, node.store, node.feed, name, node.started, out)
}

func (node *rootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	noCache(out)
	if name == ".status" {
		fileAttributes(&out.Attr, 0, node.started)
		return node.NewInode(ctx, &statusNode{store: node.store, started: node.started}, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno("/.status")}), 0
	}
	if node.store.config.Named() {
		if node.store.feeds[name] == nil {
			return nil, syscall.ENOENT
		}
		directoryAttributes(&out.Attr, node.started)
		return node.NewInode(ctx, &feedNode{store: node.store, feed: name, started: node.started}, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno("/" + name)}), 0
	}
	return lookupKind(ctx, node, node.store, "", name, node.started, out)
}

func (node *kindNode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	directoryAttributes(&out.Attr, node.started)
	out.SetTimeout(0)
	return 0
}

func (node *kindNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	symbols, err := node.store.Symbols(node.feed)
	if err != nil {
		return nil, syscall.EIO
	}
	entries := make([]fuse.DirEntry, 0, len(symbols))
	for _, symbol := range symbols {
		entries = append(entries, fuse.DirEntry{Name: symbol, Mode: syscall.S_IFDIR})
	}
	return fs.NewListDirStream(entries), 0
}

func (node *kindNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	noCache(out)
	if !symbolPattern.MatchString(name) {
		return nil, syscall.ENOENT
	}
	known, err := node.store.HasSymbol(node.feed, name)
	if err != nil {
		return nil, syscall.EIO
	}
	if !known {
		return nil, syscall.ENOENT
	}
	directoryAttributes(&out.Attr, node.started)
	return node.NewInode(ctx, &symbolNode{store: node.store, feed: node.feed, kind: node.kind, symbol: name, started: node.started}, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno("/" + joinFeed(node.feed, node.kind, "/") + "/" + name)}), 0
}

func (node *symbolNode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	directoryAttributes(&out.Attr, node.started)
	out.SetTimeout(0)
	return 0
}

func (node *symbolNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	days, err := node.store.Days(node.feed)
	if err != nil {
		return nil, syscall.EIO
	}
	var entries []fuse.DirEntry
	for _, day := range days {
		for _, extension := range []string{"parquet", "csv"} {
			entries = append(entries, fuse.DirEntry{Name: day + "." + extension, Mode: syscall.S_IFREG})
		}
	}
	return fs.NewListDirStream(entries), 0
}

func (node *symbolNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	noCache(out)
	index := strings.LastIndex(name, ".")
	if index < 0 {
		return nil, syscall.ENOENT
	}
	day, extension := name[:index], name[index+1:]
	if extension != "parquet" && extension != "csv" {
		return nil, syscall.ENOENT
	}
	days, err := node.store.Days(node.feed)
	if err != nil {
		return nil, syscall.EIO
	}
	found := false
	for _, candidate := range days {
		found = found || candidate == day
	}
	if !found {
		return nil, syscall.ENOENT
	}
	child := &fileNode{store: node.store, feed: node.feed, kind: node.kind, symbol: node.symbol, day: day, extension: extension, started: node.started}
	size, modified := child.cachedInfo()
	fileAttributes(&out.Attr, size, modified)
	return node.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno("/" + joinFeed(node.feed, node.kind, "/") + "/" + node.symbol + "/" + name)}), 0
}

func (node *fileNode) cachedInfo() (int64, time.Time) {
	path := node.store.ParquetPath(node.feed, node.kind, node.symbol, node.day)
	if node.extension == "csv" {
		path = node.store.CSVPath(node.feed, node.kind, node.symbol, node.day)
	}
	if info, err := os.Stat(path); err == nil {
		return info.Size(), info.ModTime()
	}
	return 0, node.started // size is unknown until extracted; the mount uses direct_io so readers do not rely on it
}

func (node *fileNode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.SetTimeout(0)
	if opened, ok := handle.(*fileHandle); ok { // fstat on an open file: answer from the file itself
		if info, err := opened.file.Stat(); err == nil {
			fileAttributes(&out.Attr, info.Size(), info.ModTime())
			return 0
		}
	}
	size, modified := node.cachedInfo()
	fileAttributes(&out.Attr, size, modified)
	return 0
}

type fileHandle struct {
	file    *os.File
	release func() // called once when the handle is released
}

var (
	_ fs.FileReader   = (*fileHandle)(nil)
	_ fs.FileReleaser = (*fileHandle)(nil)
)

func (handle *fileHandle) Read(ctx context.Context, destination []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	count, err := handle.file.ReadAt(destination, offset)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(destination[:count]), 0
}

func (handle *fileHandle) Release(ctx context.Context) syscall.Errno {
	handle.file.Close()
	if handle.release != nil {
		handle.release()
	}
	return 0
}

func (node *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		return nil, 0, syscall.EROFS
	}
	started := time.Now()
	for attempt := 0; ; attempt++ {
		future := node.store.Request(node.feed, node.kind, node.symbol, node.day)
		hit := future.Done()
		if node.store.config.Extract.OnMiss == "async" && !future.Done() {
			return nil, 0, syscall.EAGAIN // extraction continues in the background; retry later or watch /.status
		}
		if _, err := future.Wait(); err != nil {
			return nil, 0, syscall.EIO
		}
		if attempt == 0 {
			node.store.tel.open(node.feed, node.kind, hit, time.Since(started))
		}
		path := node.store.ParquetPath(node.feed, node.kind, node.symbol, node.day)
		var release func()
		if node.extension == "csv" {
			var err error
			if path, err = node.store.EnsureCSV(node.feed, node.kind, node.symbol, node.day); err != nil {
				return nil, 0, syscall.EIO
			}
			csvPath := path
			release = func() { node.store.ReleaseCSV(csvPath) }
		}
		file, err := os.Open(path)
		if err != nil && release != nil {
			release()
		}
		if errors.Is(err, os.ErrNotExist) && attempt == 0 {
			continue // evicted between the lookup and the open: request it again
		}
		if err != nil {
			return nil, 0, syscall.EIO
		}
		return &fileHandle{file: file, release: release}, fuse.FOPEN_DIRECT_IO, 0
	}
}

// ----- /.status --------------------------------------------------------------------------------------------------------------

type statusHandle struct {
	mutex sync.Mutex
	data  []byte
}

var _ fs.FileReader = (*statusHandle)(nil)

func (handle *statusHandle) Read(ctx context.Context, destination []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	if offset >= int64(len(handle.data)) {
		return fuse.ReadResultData(nil), 0
	}
	end := offset + int64(len(destination))
	if end > int64(len(handle.data)) {
		end = int64(len(handle.data))
	}
	return fuse.ReadResultData(handle.data[offset:end]), 0
}

func (node *statusNode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fileAttributes(&out.Attr, 0, node.started)
	out.SetTimeout(0)
	return 0
}

func (node *statusNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		return nil, 0, syscall.EROFS
	}
	data, _ := json.MarshalIndent(node.store.Status(), "", "  ")
	return &statusHandle{data: append(data, '\n')}, fuse.FOPEN_DIRECT_IO, 0
}
