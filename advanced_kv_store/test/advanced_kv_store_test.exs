defmodule AdvancedKvStore.GenServerStoreTest do
  use ExUnit.Case, async: false
  alias AdvancedKvStore.GenServerStore

  @moduletag :capture_log

  setup do
    {:ok, pid} = GenServerStore.start_link()
    GenServerStore.reset_state(pid)
    %{pid: pid}
  end

  describe "basic operations" do
    test "set and get operations", %{pid: pid} do
      assert :ok = GenServerStore.set(pid, "key1", "value1")
      assert "value1" = GenServerStore.get(pid, "key1")
    end

    test "delete operation", %{pid: pid} do
      GenServerStore.set(pid, "key2", "value2")
      assert :ok = GenServerStore.delete(pid, "key2")
      assert nil == GenServerStore.get(pid, "key2")
    end

    test "list keys and values", %{pid: pid} do
      GenServerStore.set(pid, "key3", "value3")
      GenServerStore.set(pid, "key4", "value4")
      assert ["key3", "key4"] == GenServerStore.list_keys(pid) |> Enum.sort()
      assert ["value3", "value4"] == GenServerStore.list_values(pid) |> Enum.sort()
    end
  end

  describe "TTL functionality" do
    test "set with TTL", %{pid: pid} do
      GenServerStore.set(pid, "expire_soon", "value", 100)
      assert "value" == GenServerStore.get(pid, "expire_soon")
      :timer.sleep(150)
      assert nil == GenServerStore.get(pid, "expire_soon")
    end

    test "update TTL", %{pid: pid} do
      GenServerStore.set(pid, "update_ttl", "value", 1000)
      GenServerStore.update_ttl(pid, "update_ttl", 100)
      :timer.sleep(150)
      assert nil == GenServerStore.get(pid, "update_ttl")
    end

    test "clear expired keys", %{pid: pid} do
      GenServerStore.set(pid, "expire1", "value1", 100)
      GenServerStore.set(pid, "expire2", "value2", 100)
      GenServerStore.set(pid, "keep", "value3")
      :timer.sleep(150)
      assert 2 == GenServerStore.clear_expired(pid)
      assert ["keep"] == GenServerStore.list_keys(pid)
    end
  end

  describe "persistence" do
    test "save and load state", %{pid: pid} do
      GenServerStore.set(pid, "persist1", "value1")
      GenServerStore.set(pid, "persist2", "value2")

      # Stop the GenServer
      GenServer.stop(pid)

      # Start a new GenServer, which should load the saved state
      {:ok, new_pid} = GenServerStore.start_link()

      assert "value1" == GenServerStore.get(new_pid, "persist1")
      assert "value2" == GenServerStore.get(new_pid, "persist2")
    end
  end

  describe "concurrent access" do
    test "multiple clients setting and getting concurrently", %{pid: pid} do
      tasks = for i <- 1..100 do
        Task.async(fn ->
          GenServerStore.set(pid, "key#{i}", "value#{i}")
          GenServerStore.get(pid, "key#{i}")
        end)
      end

      results = Task.await_many(tasks)
      assert length(results) == 100
      assert Enum.all?(results, &(&1 != nil))
    end
  end

  describe "error handling" do
    test "get non-existent key", %{pid: pid} do
      assert nil == GenServerStore.get(pid, "non_existent")
    end

    test "update TTL for non-existent key", %{pid: pid} do
      assert {:error, :not_found} = GenServerStore.update_ttl(pid, "non_existent", 1000)
    end

    test "delete non-existent key", %{pid: pid} do
      assert :ok = GenServerStore.delete(pid, "non_existent")
    end
  end

  describe "edge cases" do
    test "set and get with empty string key", %{pid: pid} do
      assert :ok = GenServerStore.set(pid, "", "empty key value")
      assert "empty key value" = GenServerStore.get(pid, "")
    end

    test "set and get with very long key", %{pid: pid} do
      long_key = String.duplicate("a", 1000)
      assert :ok = GenServerStore.set(pid, long_key, "long key value")
      assert "long key value" = GenServerStore.get(pid, long_key)
    end

    test "set with very short TTL", %{pid: pid} do
      GenServerStore.set(pid, "short_ttl", "value", 1)
      :timer.sleep(10)
      assert nil == GenServerStore.get(pid, "short_ttl")
    end
  end

  describe "stress tests" do
    test "large number of operations", %{pid: pid} do
      for i <- 1..10_000 do
        assert :ok = GenServerStore.set(pid, "key#{i}", "value#{i}")
      end

      for i <- 1..10_000 do
        expected_value = "value#{i}"
        assert ^expected_value = GenServerStore.get(pid, "key#{i}")
      end

      assert 10_000 == length(GenServerStore.list_keys(pid))
    end

    test "rapid TTL updates", %{pid: pid} do
      GenServerStore.set(pid, "rapid_ttl", "initial", 10_000)

      for _ <- 1..1000 do
        new_ttl = :rand.uniform(10_000)
        GenServerStore.update_ttl(pid, "rapid_ttl", new_ttl)
      end

      assert "initial" = GenServerStore.get(pid, "rapid_ttl")
    end
  end

  describe "batch operations" do
    test "perform multiple operations in a single call", %{pid: pid} do
      operations = [
        {:set, "batch_key1", "value1", :infinity},
        {:set, "batch_key2", "value2", 60000},
        {:get, "batch_key1"},
        {:delete, "batch_key1"},
        {:get, "batch_key1"},
        {:get, "batch_key2"}
      ]

      results = GenServerStore.batch(pid, operations)

      assert results == [:ok, :ok, "value1", :ok, nil, "value2"]
    end

    test "batch operations with TTL", %{pid: pid} do
      operations = [
        {:set, "ttl_key1", "value1", 100},
        {:set, "ttl_key2", "value2", :infinity},
        {:get, "ttl_key1"},
        {:get, "ttl_key2"}
      ]

      results = GenServerStore.batch(pid, operations)
      assert results == [:ok, :ok, "value1", "value2"]

      :timer.sleep(150)

      results = GenServerStore.batch(pid, [
        {:get, "ttl_key1"},
        {:get, "ttl_key2"}
      ])

      assert results == [nil, "value2"]
    end
  end

  describe "pub/sub mechanism" do
    test "subscribe and receive updates", %{pid: pid} do
      GenServerStore.subscribe(pid, "test_key")
      GenServerStore.set(pid, "test_key", "initial_value")

      assert_receive {:kv_update, "test_key", {:set, "initial_value"}}, 100

      GenServerStore.set(pid, "test_key", "updated_value")
      assert_receive {:kv_update, "test_key", {:set, "updated_value"}}, 100

      GenServerStore.delete(pid, "test_key")
      assert_receive {:kv_update, "test_key", :deleted}, 100
    end

    test "unsubscribe and stop receiving updates", %{pid: pid} do
      GenServerStore.subscribe(pid, "unsub_key")
      GenServerStore.set(pid, "unsub_key", "value1")
      assert_receive {:kv_update, "unsub_key", {:set, "value1"}}, 100

      GenServerStore.unsubscribe(pid, "unsub_key")
      GenServerStore.set(pid, "unsub_key", "value2")
      refute_receive {:kv_update, "unsub_key", _}, 100
    end

    test "receive expiration notification", %{pid: pid} do
      GenServerStore.subscribe(pid, "expire_key")
      GenServerStore.set(pid, "expire_key", "expiring_value", 100)
      assert_receive {:kv_update, "expire_key", {:set, "expiring_value"}}, 100
      assert_receive {:kv_update, "expire_key", :expired}, 200
    end
  end

  describe "nested data structures" do
    test "set and get nested data", %{pid: pid} do
      GenServerStore.set_nested(pid, ["users", "123", "name"], "John Doe")
      assert "John Doe" == GenServerStore.get_nested(pid, ["users", "123", "name"])
    end

    test "nested data with TTL", %{pid: pid} do
      GenServerStore.set_nested(pid, ["temp", "data"], "expires soon", 100)
      assert "expires soon" == GenServerStore.get_nested(pid, ["temp", "data"])
      :timer.sleep(150)
      assert nil == GenServerStore.get_nested(pid, ["temp", "data"])
    end

    test "update nested data", %{pid: pid} do
      GenServerStore.set_nested(pid, ["config", "database", "url"], "localhost")
      assert "localhost" == GenServerStore.get_nested(pid, ["config", "database", "url"])

      GenServerStore.set_nested(pid, ["config", "database", "url"], "new_server")
      assert "new_server" == GenServerStore.get_nested(pid, ["config", "database", "url"])
    end

    test "delete nested data", %{pid: pid} do
      GenServerStore.set_nested(pid, ["to_delete", "nested", "key"], "value")
      assert "value" == GenServerStore.get_nested(pid, ["to_delete", "nested", "key"])

      GenServerStore.delete(pid, "to_delete")
      assert nil == GenServerStore.get_nested(pid, ["to_delete", "nested", "key"])
    end

    test "nested data with pub/sub", %{pid: pid} do
      GenServerStore.subscribe(pid, ["users", "456", "email"])
      GenServerStore.set_nested(pid, ["users", "456", "email"], "user@example.com")

      assert_receive {:kv_update, ["users", "456", "email"], {:set, "user@example.com"}}, 100

      GenServerStore.set_nested(pid, ["users", "456", "email"], "newemail@example.com")
      assert_receive {:kv_update, ["users", "456", "email"], {:set, "newemail@example.com"}}, 100
    end
  end
end
