package main

import "testing"

// TestNewCLI_Defaults verifies address defaulting (scheduler/schema fall back
// to the server address) and trailing-slash trimming.
func TestNewCLI_Defaults(t *testing.T) {
	t.Run("defaults to server", func(t *testing.T) {
		c := newCLI("http://host:8080/", "", "")
		if c.server != "http://host:8080" {
			t.Errorf("server = %q, want trimmed", c.server)
		}
		if c.scheduler != "http://host:8080" {
			t.Errorf("scheduler = %q, want fallback to server", c.scheduler)
		}
		if c.schema != "http://host:8080" {
			t.Errorf("schema = %q, want fallback to server", c.schema)
		}
	})

	t.Run("explicit addresses", func(t *testing.T) {
		c := newCLI("http://s/", "http://sched/", "http://schm/")
		if c.server != "http://s" || c.scheduler != "http://sched" || c.schema != "http://schm" {
			t.Errorf("got %+v, want trimmed explicit addresses", c)
		}
	})
}
