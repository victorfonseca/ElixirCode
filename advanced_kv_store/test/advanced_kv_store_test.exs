defmodule AdvancedKvStore.GenServerStoreTest do
  use ExUnit.Case, async: false
  alias AdvancedKvStore.GenServerStore

  @moduletag :capture_log

  setup do
    # Start a new GenServerStore for each test
    {:ok, pid} = GenServerStore.start_link()
    # Ensure we start with a clean state
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
end
