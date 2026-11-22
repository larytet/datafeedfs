package main

import (
	"context"
	"encoding/json"
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

// Layout:
//   /                               symbols of the newest day, plus .status
//   /<SYMBOL>/<YYYY-MM-DD>.parquet  tick-to-tick top of book and trades for that symbol and day
//   /<SYMBOL>/<YYYY-MM-DD>.csv      the same as CSV
//   /.status                        JSON: running and pending extractions, failures
// Opening a file that is not cached yet triggers the extraction (Store.Request).

type rootNode struct {
	fs.Inode
	store   *Store
	started time.Time
}

type symbolNode struct {
	fs.Inode
	store   *Store
	symbol  string
	started time.Time
}

type fileNode struct {
	fs.Inode
	store     *Store
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
	symbols, err := node.store.Symbols()
	if err != nil {
		return nil, syscall.EIO
	}
	entries := []fuse.DirEntry{{Name: ".status", Mode: syscall.S_IFREG}}
	for _, symbol := range symbols {
		entries = append(entries, fuse.DirEntry{Name: symbol, Mode: syscall.S_IFDIR})
	}
	return fs.NewListDirStream(entries), 0
}

func (node *rootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	noCache(out)
	if name == ".status" {
		fileAttributes(&out.Attr, 0, node.started)
		return node.NewInode(ctx, &statusNode{store: node.store, started: node.started}, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno("/.status")}), 0
	}
	if !symbolPattern.MatchString(name) {
		return nil, syscall.ENOENT
	}
	known, err := node.store.HasSymbol(name)
	if err != nil {
		return nil, syscall.EIO
	}
	if !known {
		return nil, syscall.ENOENT
	}
	directoryAttributes(&out.Attr, node.started)
	return node.NewInode(ctx, &symbolNode{store: node.store, symbol: name, started: node.started}, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno("/" + name)}), 0
}

func (node *symbolNode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	directoryAttributes(&out.Attr, node.started)
	out.SetTimeout(0)
	return 0
}

func (node *symbolNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	days, err := node.store.Days()
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
	days, err := node.store.Days()
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
	child := &fileNode{store: node.store, symbol: node.symbol, day: day, extension: extension, started: node.started}
	size, modified := child.cachedInfo()
	fileAttributes(&out.Attr, size, modified)
	return node.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno("/" + node.symbol + "/" + name)}), 0
}

func (node *fileNode) cachedInfo() (int64, time.Time) {
	path := node.store.ParquetPath(node.symbol, node.day)
	if node.extension == "csv" {
		path = node.store.CSVPath(node.symbol, node.day)
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

type fileHandle struct{ file *os.File }

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
	return 0
}

func (node *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0 {
		return nil, 0, syscall.EROFS
	}
	future := node.store.Request(node.symbol, node.day)
	if node.store.config.Extract.OnMiss == "async" && !future.Done() {
		return nil, 0, syscall.EAGAIN // extraction continues in the background; retry later or watch /.status
	}
	if _, err := future.Wait(); err != nil {
		return nil, 0, syscall.EIO
	}
	path := node.store.ParquetPath(node.symbol, node.day)
	if node.extension == "csv" {
		var err error
		if path, err = node.store.EnsureCSV(node.symbol, node.day); err != nil {
			return nil, 0, syscall.EIO
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return &fileHandle{file: file}, fuse.FOPEN_DIRECT_IO, 0
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
