package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type uiTest struct {
	t      *testing.T
	server *httptest.Server
	store  *Store
	path   string
}

func newUITest(t *testing.T, extra string) *uiTest {
	t.Helper()
	store, path := newTestStore(t, extra)
	store.sampler = newSampler(store)
	ui := &uiServer{store: store, mux: http.NewServeMux()}
	ui.routes()
	server := httptest.NewServer(ui)
	t.Cleanup(server.Close)
	return &uiTest{t, server, store, path}
}

// call sends a request and decodes the JSON answer (or returns the raw body in "body" for other types).
func (u *uiTest) call(method, path string, body any, headers map[string]string) (int, map[string]any) {
	u.t.Helper()
	var reader io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reader = bytes.NewReader(data)
	}
	request, _ := http.NewRequest(method, u.server.URL+path, reader)
	if method != http.MethodGet {
		request.Header.Set("X-Requested-With", "datafeedfs-ui")
	}
	for key, value := range headers {
		if key == "Host" {
			request.Host = value
		} else if value == "" {
			request.Header.Del(key)
		} else {
			request.Header.Set(key, value)
		}
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		u.t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	out := map[string]any{}
	if json.Unmarshal(data, &out) != nil {
		out["body"] = string(data)
	}
	return response.StatusCode, out
}

func TestUIPageAndInfo(t *testing.T) {
	u := newUITest(t, "")
	status, page := u.call("GET", "/", nil, nil)
	if status != 200 || !strings.Contains(page["body"].(string), "<title>datafeedfs") {
		t.Fatalf("page: %d %v", status, page)
	}
	response, _ := http.Get(u.server.URL + "/")
	if !strings.Contains(response.Header.Get("Content-Security-Policy"), "default-src 'self'") {
		t.Error("no content security policy")
	}
	if _, info := u.call("GET", "/api/info", nil, nil); info["auth_required"] != false {
		t.Errorf("info: %v", info)
	}
}

func TestUIStatusAndMetrics(t *testing.T) {
	u := newUITest(t, "")
	path := u.store.ParquetPath(kindTick, "AAA", "2024-01-02")
	os.MkdirAll(strings.TrimSuffix(path, "/2024-01-02.parquet"), 0o755)
	os.WriteFile(path, make([]byte, 1234), 0o644)
	u.store.scanCache()
	u.store.Request(kindTick, "AAA", "2024-01-02").Wait()

	status, body := u.call("GET", "/api/status", nil, nil)
	if status != 200 {
		t.Fatal(status, body)
	}
	cache := body["cache"].(map[string]any)
	tick := cache["by_kind"].(map[string]any)["tick"].(map[string]any)
	if tick["bytes"].(float64) != 1234 || cache["files"].(float64) != 1 {
		t.Errorf("cache: %v", cache)
	}
	passes := body["passes"].(map[string]any)
	if passes["slots"].(float64) != 2 || len(passes["running"].([]any)) != 0 {
		t.Errorf("passes: %v", passes)
	}
	mount := body["mount"].(map[string]any)
	if mount["version"] == "" || mount["allow_edit"] != false || mount["source"] != "this machine" {
		t.Errorf("mount: %v", mount)
	}

	u.store.sampler.sample()
	time.Sleep(1100 * time.Millisecond)
	u.store.sampler.sample()
	status, metrics := u.call("GET", "/api/metrics?range=5m", nil, nil)
	if status != 200 {
		t.Fatal(status, metrics)
	}
	for _, key := range []string{"t", "net_mbs", "cpu_total", "load1", "mem_gib", "cache_write_mbs", "hit_ratio", "cpu", "passes", "processes", "host"} {
		if _, ok := metrics[key]; !ok {
			t.Errorf("metrics has no %q", key)
		}
	}
	if len(metrics["t"].([]any)) < 1 {
		t.Error("no samples")
	}
	if cpu := metrics["cpu"].(map[string]any); cpu["xz"] == nil || cpu["other"] == nil {
		t.Errorf("cpu groups: %v", cpu)
	}
	if hit := metrics["hit_ratio"].([]any); len(hit) > 0 && hit[len(hit)-1] != nil {
		t.Logf("hit ratio %v", hit[len(hit)-1])
	}
}

func TestUIConfigReadValidateSave(t *testing.T) {
	u := newUITest(t, "[ui]\nallow_edit = true\n")
	status, current := u.call("GET", "/api/config", nil, nil)
	if status != 200 || current["etag"] != current["applied_etag"] || !strings.Contains(current["text"].(string), "[extract]") {
		t.Fatalf("get: %d %v", status, current)
	}
	text := current["text"].(string)

	// validation: semantic errors are reported without saving
	status, bad := u.call("POST", "/api/config/validate", map[string]string{"text": strings.Replace(text, "[cache]\n", "[cache]\nevict_to = 5\n", 1)}, nil)
	if status != 400 || !strings.Contains(bad["error"].(string), "evict_to") || strings.Contains(bad["error"].(string), u.path) {
		t.Errorf("validate: %d %v", status, bad)
	}
	if status, ok := u.call("POST", "/api/config/validate", map[string]string{"text": text}, nil); status != 200 || ok["ok"] != true {
		t.Errorf("validate good: %d %v", status, ok)
	}

	// saving needs the header, the right etag and a valid file
	if status, _ := u.call("PUT", "/api/config", map[string]string{"text": text, "etag": current["etag"].(string)}, map[string]string{"X-Requested-With": ""}); status != 403 {
		t.Errorf("without X-Requested-With: %d", status)
	}
	if status, _ := u.call("PUT", "/api/config", map[string]string{"text": text, "etag": current["etag"].(string)}, map[string]string{"Origin": "http://evil.test"}); status != 403 {
		t.Errorf("cross-origin: %d", status)
	}
	if status, _ := u.call("PUT", "/api/config", map[string]string{"text": text, "etag": "0000"}, nil); status != 409 {
		t.Errorf("stale etag: %d", status)
	}
	if status, _ := u.call("PUT", "/api/config", map[string]string{"text": "[extract\nmax_parallel = 3", "etag": current["etag"].(string)}, nil); status != 400 {
		t.Errorf("broken toml: %d", status)
	}
	if data, _ := os.ReadFile(u.path); string(data) != text {
		t.Fatal("the file changed although nothing valid was saved")
	}
	edited := strings.Replace(text, "batch_window_seconds = 0.05", "batch_window_seconds = 0.05\nmax_parallel = 4", 1)
	status, saved := u.call("PUT", "/api/config", map[string]string{"text": edited, "etag": current["etag"].(string)}, nil)
	if status != 200 || saved["etag"] == current["etag"] || saved["applied_etag"] != current["applied_etag"] {
		t.Fatalf("save: %d %v", status, saved)
	}
	if data, _ := os.ReadFile(u.path); string(data) != edited {
		t.Error("file content differs from what was saved")
	}
	if u.store.config.Extract.MaxParallel == 4 {
		t.Error("the running config must not change before a restart")
	}
}

func TestUIReadOnlyAndRestartForbidden(t *testing.T) {
	u := newUITest(t, "")
	_, current := u.call("GET", "/api/config", nil, nil)
	if status, body := u.call("PUT", "/api/config", map[string]string{"text": current["text"].(string), "etag": current["etag"].(string)}, nil); status != 403 || !strings.Contains(body["error"].(string), "allow_edit") {
		t.Errorf("read-only put: %d %v", status, body)
	}
	if status, body := u.call("POST", "/api/restart", nil, nil); status != 403 || !strings.Contains(body["error"].(string), "allow_restart") {
		t.Errorf("restart: %d %v", status, body)
	}
}

func TestUITokenAndHostChecks(t *testing.T) {
	u := newUITest(t, "[ui]\ntoken = \"s3cret\"\n")
	if _, info := u.call("GET", "/api/info", nil, nil); info["auth_required"] != true {
		t.Errorf("info: %v", info)
	}
	for _, path := range []string{"/api/status", "/api/config", "/api/metrics"} {
		if status, _ := u.call("GET", path, nil, nil); status != 401 {
			t.Errorf("%s without token: %d", path, status)
		}
		if status, _ := u.call("GET", path, nil, map[string]string{"Authorization": "Bearer wrong"}); status != 401 {
			t.Errorf("%s wrong token: %d", path, status)
		}
		if status, _ := u.call("GET", path, nil, map[string]string{"Authorization": "Bearer s3cret"}); status != 200 {
			t.Errorf("%s right token: %d", path, status)
		}
	}
	if status, _ := u.call("GET", "/", nil, nil); status != 200 {
		t.Errorf("the page itself needs no token (it asks for one): %d", status)
	}

	// no token: only loopback Host names are served (DNS rebinding)
	open := newUITest(t, "")
	if status, _ := open.call("GET", "/api/status", nil, map[string]string{"Host": "evil.example.com"}); status != 403 {
		t.Errorf("foreign Host: %d", status)
	}
	if status, _ := open.call("GET", "/api/status", nil, map[string]string{"Host": "localhost:8088"}); status != 200 {
		t.Errorf("localhost Host: %d", status)
	}
}
