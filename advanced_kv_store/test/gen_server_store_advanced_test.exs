defmodule AdvancedKvStore.GenServerStoreAdvancedTest do
  use ExUnit.Case, async: false
  alias AdvancedKvStore.GenServerStore

  @moduletag :capture_log

  setup do
    {:ok, pid} = GenServerStore.start_link()
    GenServerStore.reset_state(pid)
    %{pid: pid}
  end

  setup do
    {:ok, pid} = GenServerStore.start_link()
    GenServerStore.reset_state(pid)
    %{pid: pid}
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

    test "update TTL of non-existent key", %{pid: pid} do
      assert {:error, :not_found} = GenServerStore.update_ttl(pid, "non_existent", 1000)
    end

    test "delete non-existent key", %{pid: pid} do
      assert :ok = GenServerStore.delete(pid, "non_existent")
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

    test "concurrent operations", %{pid: pid} do
      tasks = for i <- 1..1000 do
        Task.async(fn ->
          GenServerStore.set(pid, "concurrent_key#{i}", "value#{i}")
          GenServerStore.get(pid, "concurrent_key#{i}")
        end)
      end

      results = Task.await_many(tasks, 30_000)
      assert length(results) == 1000
      assert Enum.all?(results, &(&1 == "value#{Enum.find_index(results, fn r -> r == &1 end) + 1}"))
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

  describe "persistence tests" do
    test "data survives process restart", %{pid: pid} do
      GenServerStore.set(pid, "persist1", "value1")
      GenServerStore.set(pid, "persist2", "value2", 60_000)

      # Stop the GenServer
      GenServer.stop(pid)

      # Start a new GenServer
      {:ok, new_pid} = GenServerStore.start_link()

      assert "value1" = GenServerStore.get(new_pid, "persist1")
      assert "value2" = GenServerStore.get(new_pid, "persist2")
    end

    test "expired data is not loaded after restart", %{pid: pid} do
      GenServerStore.set(pid, "expire_soon", "value", 100)
      :timer.sleep(150)

      # Stop the GenServer
      GenServer.stop(pid)

      # Start a new GenServer
      {:ok, new_pid} = GenServerStore.start_link()

      assert nil == GenServerStore.get(new_pid, "expire_soon")
    end
  end
end
