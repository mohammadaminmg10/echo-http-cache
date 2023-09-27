package cache

/*
MIT License

Copyright (c) 2018 Victor Springer

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

// Response is the cached response data structure.
type Response struct {
	// Value is the cached response value.
	Value []byte

	// Header is the cached response header.
	Header http.Header

	// Expiration is the cached response expiration date.
	Expiration time.Time

	// LastAccess is the last date a cached response was accessed.
	// Used by LRU and MRU algorithms.
	LastAccess time.Time

	// Frequency is the count of times a cached response is accessed.
	// Used for LFU and MFU algorithms.
	Frequency int
}

// Client data structure for HTTP cache middleware.
type Client struct {
	adapter         Adapter
	ttl             time.Duration
	refreshKey      string
	methods         []string
	restrictedPaths []string
}

type bodyDumpResponseWriter struct {
	io.Writer
	http.ResponseWriter
	statusCode int
}

func (w *bodyDumpResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *bodyDumpResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w *bodyDumpResponseWriter) Flush() {
	w.ResponseWriter.(http.Flusher).Flush()
}

func (w *bodyDumpResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}

// ClientOption is used to set Client settings.
type ClientOption func(c *Client) error

// Adapter interface for HTTP cache middleware client.
type Adapter interface {
	// Get retrieves the cached response by a given key. It also
	// returns true or false, whether it exists or not.
	Get(key uint64) ([]byte, bool)

	// Set caches a response for a given key until an expiration date.
	Set(key uint64, response []byte, expiration time.Time)

	// Release frees cache for a given key.
	Release(key uint64)
}

// Middleware is the HTTP cache middleware handler.
func (client *Client) Middleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Request().URL.ForceQuery = false
			if !client.isAllowedPathToCache(c.Request().URL.String()) {
				next(c)
				return nil
			}
			if client.cacheableMethod(c.Request().Method) {
				sortURLParams(c.Request().URL)
				key := generateKey(c.Request().URL.String())
				if c.Request().Method == http.MethodPost && c.Request().Body != nil {
					body, err := ioutil.ReadAll(c.Request().Body)
					defer c.Request().Body.Close()
					if err != nil {
						next(c)
						return nil
					}
					reader := ioutil.NopCloser(bytes.NewBuffer(body))
					key = generateKeyWithBody(c.Request().URL.String(), body)
					c.Request().Body = reader
				}

				params := c.Request().URL.Query()
				if _, ok := params[client.refreshKey]; ok {
					delete(params, client.refreshKey)

					c.Request().URL.RawQuery = params.Encode()
					key = generateKey(c.Request().URL.String())

				} else {
					b, ok := client.adapter.Get(key)
					response := BytesToResponse(b)
					if ok {
						if response.Expiration.After(time.Now()) {
							response.LastAccess = time.Now()
							response.Frequency++
							client.adapter.Set(key, response.Bytes(), response.Expiration)

							//w.WriteHeader(http.StatusNotModified)
							for k, v := range response.Header {
								c.Response().Header().Set(k, strings.Join(v, ","))
							}
							c.Response().WriteHeader(http.StatusOK)
							c.Response().Write(response.Value)
							return nil
						}
					}
				}

				resBody := new(bytes.Buffer)
				mw := io.MultiWriter(c.Response().Writer, resBody)
				writer := &bodyDumpResponseWriter{Writer: mw, ResponseWriter: c.Response().Writer}
				c.Response().Writer = writer
				if err := next(c); err != nil {
					c.Error(err)
				}

				statusCode := writer.statusCode
				value := resBody.Bytes()
				if statusCode < 400 {
					now := time.Now()

					response := Response{
						Value:      value,
						Header:     writer.Header(),
						Expiration: now.Add(client.ttl),
						LastAccess: now,
						Frequency:  1,
					}
					client.adapter.Release(key)
					client.adapter.Set(key, response.Bytes(), response.Expiration)
				}
				//for k, v := range writer.Header() {
				//	c.Response().Header().Set(k, strings.Join(v, ","))
				//}
				//c.Response().WriteHeader(statusCode)
				//c.Response().Write(value)
				return nil
			}
			if err := next(c); err != nil {
				c.Error(err)
			}
			return nil
		}
	}
}

func (c *Client) cacheableMethod(method string) bool {
	for _, m := range c.methods {
		if method == m {
			return true
		}
	}
	return false
}

func (c *Client) isAllowedPathToCache(URL string) bool {
	for _, p := range c.restrictedPaths {
		if strings.Contains(URL, p) {
			return false
		}
	}
	return true
}

// BytesToResponse converts bytes array into Response data structure.
func BytesToResponse(b []byte) Response {
	var r Response
	dec := gob.NewDecoder(bytes.NewReader(b))
	dec.Decode(&r)

	return r
}

// Bytes converts Response data structure into bytes array.
func (r Response) Bytes() []byte {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	enc.Encode(&r)

	return b.Bytes()
}

func sortURLParams(URL *url.URL) {
	params := URL.Query()
	for _, param := range params {
		sort.Slice(param, func(i, j int) bool {
			return param[i] < param[j]
		})
	}
	URL.RawQuery = params.Encode()
}

// KeyAsString can be used by adapters to convert the cache key from uint64 to string.
func KeyAsString(key uint64) string {
	return strconv.FormatUint(key, 36)
}

func generateKey(URL string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(URL))

	return hash.Sum64()
}

func generateKeyWithBody(URL string, body []byte) uint64 {
	hash := fnv.New64a()
	body = append([]byte(URL), body...)
	hash.Write(body)

	return hash.Sum64()
}

// NewClient initializes the cache HTTP middleware client with the given
// options.
func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	if c.adapter == nil {
		return nil, errors.New("cache client adapter is not set")
	}
	if int64(c.ttl) < 1 {
		return nil, errors.New("cache client ttl is not set")
	}
	if c.methods == nil {
		c.methods = []string{http.MethodGet}
	}

	return c, nil
}

// ClientWithAdapter sets the adapter type for the HTTP cache
// middleware client.
func ClientWithAdapter(a Adapter) ClientOption {
	return func(c *Client) error {
		c.adapter = a
		return nil
	}
}

// ClientWithTTL sets how long each response is going to be cached.
func ClientWithTTL(ttl time.Duration) ClientOption {
	return func(c *Client) error {
		if int64(ttl) < 1 {
			return fmt.Errorf("cache client ttl %v is invalid", ttl)
		}

		c.ttl = ttl

		return nil
	}
}

// ClientWithRefreshKey sets the parameter key used to free a request
// cached response. Optional setting.
func ClientWithRefreshKey(refreshKey string) ClientOption {
	return func(c *Client) error {
		c.refreshKey = refreshKey
		return nil
	}
}

// ClientWithMethods sets the acceptable HTTP methods to be cached.
// Optional setting. If not set, default is "GET".
func ClientWithMethods(methods []string) ClientOption {
	return func(c *Client) error {
		for _, method := range methods {
			if method != http.MethodGet && method != http.MethodPost {
				return fmt.Errorf("invalid method %s", method)
			}
		}
		c.methods = methods
		return nil
	}
}

// ClientWithRestrictedPaths sets the restricted HTTP paths for caching.
// Optional setting.
func ClientWithRestrictedPaths(paths []string) ClientOption {
	return func(c *Client) error {
		c.restrictedPaths = paths
		return nil
	}
}
