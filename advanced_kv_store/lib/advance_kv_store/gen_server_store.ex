defmodule AdvancedKvStore.GenServerStore do
  @moduledoc """
  A GenServer-based key-value store with support for TTL (time-to-live), persistence, batch operations, pub/sub, and nested data structures.

  This module provides a key-value store that allows setting, getting, and deleting keys with optional TTL.
  The state is periodically saved to disk for persistence.

  Features:
  - Set and get key-value pairs
  - Optional TTL (Time-To-Live) for entries
  - Persistence to disk
  - Batch operations
  - Pub/Sub mechanism for key updates
  - Support for nested data structures
  - Automatic clearing of expired keys
  """

  use GenServer
  require Logger

  @default_persistence_file "kv_store.dat"
  @save_interval :timer.minutes(1)

  # Client API

  @doc """
  Starts the GenServerStore.

  ## Options

    * `:name` - The name to register the GenServer process.

  ## Examples

      iex> {:ok, pid} = AdvancedKvStore.GenServerStore.start_link(name: :kv_store)
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @doc """
  Gets the current persistence file path.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.get_persistence_file()
      "kv_store.dat"
  """
  def get_persistence_file do
    GenServer.call(__MODULE__, :get_persistence_file)
  end

  @doc """
  Sets a new persistence file path.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.set_persistence_file("new_store.dat")
      :ok
  """
  def set_persistence_file(new_file) do
    GenServer.call(__MODULE__, {:set_persistence_file, new_file})
  end

  @doc """
  Clears all expired keys from the store.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.clear_expired(pid)
      2
  """
  def clear_expired(pid) do
    GenServer.call(pid, :clear_expired)
  end

  @doc """
  Sets a key-value pair with an optional TTL.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.set(pid, "key", "value", 60000)
      :ok
  """
  def set(pid, key, value, ttl \\ :infinity) do
    GenServer.call(pid, {:set, key, value, ttl})
  end

  @doc """
  Updates the TTL for a given key.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.update_ttl(pid, "key", 120000)
      :ok
  """
  def update_ttl(pid, key, new_ttl) do
    GenServer.call(pid, {:update_ttl, key, new_ttl})
  end

  @doc """
  Gets the value for a given key.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.get(pid, "key")
      "value"
  """
  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  @doc """
  Deletes a key from the store.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.delete(pid, "key")
      :ok
  """
  def delete(pid, key) do
    GenServer.call(pid, {:delete, key})
  end

  @doc """
  Lists all keys in the store.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.list_keys(pid)
      ["key1", "key2"]
  """
  def list_keys(pid) do
    GenServer.call(pid, :list_keys)
  end

  @doc """
  Lists all values in the store.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.list_values(pid)
      ["value1", "value2"]
  """
  def list_values(pid) do
    GenServer.call(pid, :list_values)
  end

  @doc """
  Performs multiple operations in a single call.

  ## Parameters

  - `pid`: The PID of the GenServer.
  - `operations`: A list of operations to perform. Each operation is a tuple:
    - `{:set, key, value, ttl}` for setting a key-value pair
    - `{:get, key}` for getting a value
    - `{:delete, key}` for deleting a key

  ## Returns

  A list of results, one for each operation, in the same order as the input.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.batch(pid, [
      ...>   {:set, "key1", "value1", :infinity},
      ...>   {:get, "key1"},
      ...>   {:set, "key2", "value2", 60000},
      ...>   {:delete, "key1"}
      ...> ])
      [:ok, "value1", :ok, :ok]
  """
  def batch(pid, operations) do
    GenServer.call(pid, {:batch, operations})
  end

  @doc """
  Resets the state of the GenServerStore.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.reset_state(pid)
      :ok
  """
  def reset_state(pid) do
    GenServer.call(pid, :reset_state)
  end

  @doc """
  Subscribes the caller to updates for a specific key.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.subscribe(pid, "key1")
      :ok
  """
  def subscribe(pid, key) do
    GenServer.call(pid, {:subscribe, key, self()})
  end

  @doc """
  Unsubscribes the caller from updates for a specific key.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.unsubscribe(pid, "key1")
      :ok
  """
  def unsubscribe(pid, key) do
    GenServer.call(pid, {:unsubscribe, key, self()})
  end

  @doc """
  Sets a nested key-value pair with an optional TTL.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.set_nested(pid, ["users", "123", "name"], "John Doe", 60000)
      :ok
  """
  def set_nested(pid, keys, value, ttl \\ :infinity) when is_list(keys) do
    GenServer.call(pid, {:set_nested, keys, value, ttl})
  end

  @doc """
  Gets the value for a nested key.

  ## Examples

      iex> AdvancedKvStore.GenServerStore.get_nested(pid, ["users", "123", "name"])
      "John Doe"
  """
  def get_nested(pid, keys) when is_list(keys) do
    GenServer.call(pid, {:get_nested, keys})
  end

  # Server Callbacks

  @impl true
  def init(:ok) do
    Logger.debug("GenServerStore initialized")
    state = load_state(@default_persistence_file)
    state = Map.put(state, :subscribers, %{})
    schedule_save()
    {:ok, Map.put(state, :persistence_file, @default_persistence_file)}
  end


  @impl true
  def handle_call(:get_persistence_file, _from, state) do
    {:reply, state.persistence_file, state}
  end

  def handle_call(:reset_state, _from, state) do
    File.rm(state.persistence_file)
    new_state = %{state | data: %{}, timers: %{}, subscribers: %{}}
    {:reply, :ok, new_state}
  end

  def handle_call({:set_persistence_file, new_file}, _from, state) do
    {:reply, :ok, %{state | persistence_file: new_file}}
  end

  def handle_call({:set, key, value, ttl}, _from, state) do
    new_state = do_set(key, value, ttl, state)
    {:reply, :ok, new_state}
  end

  def handle_call({:delete, key}, _from, state) do
    new_state = do_delete(key, state)
    {:reply, :ok, new_state}
  end

  def handle_call(:list_keys, _from, state) do
    keys = Map.keys(state.data)
    {:reply, keys, state}
  end

  def handle_call({:update_ttl, key, new_ttl}, _from, state) do
    case Map.get(state.data, key) do
      nil ->
        {:reply, {:error, :not_found}, state}

      {value, _old_expiry} ->
        new_state = do_set(key, value, new_ttl, state)
        {:reply, :ok, new_state}
    end
  end

  def handle_call({:get, key}, _from, state) do
    {result, new_state} = do_get(key, state)
    {:reply, result, new_state}
  end

  def handle_call(:clear_expired, _from, state) do
    now = current_time()
    {expired, valid} = Enum.split_with(state.data, fn {_key, {_value, expiry}} ->
      expiry == :expired or (expiry != :infinity and expiry <= now)
    end)

    new_data = Map.new(valid)
    expired_keys = Enum.map(expired, fn {key, _} -> key end)

    new_timers = Map.drop(state.timers, expired_keys)
    new_state = %{state | data: new_data, timers: new_timers}

    save_state(new_state)

    {:reply, length(expired_keys), new_state}
  end

  def handle_call(:list_values, _from, state) do
    values =
      state.data
      |> Map.values()
      |> Enum.map(fn {value, expiry} ->
        if expiry == :infinity or expiry > current_time() do
          value
        else
          nil
        end
      end)
      |> Enum.reject(&is_nil/1)

    {:reply, values, state}
  end

  def handle_call({:batch, operations}, _from, state) do
    {results, new_state} =
      Enum.reduce(operations, {[], state}, fn operation, {acc_results, acc_state} ->
        {result, new_state} = process_operation(operation, acc_state)
        {acc_results ++ [result], new_state}
      end)

    save_state(new_state)
    {:reply, results, new_state}
  end

  def handle_call({:subscribe, key, subscriber}, _from, state) do
    subscribers = Map.get(state.subscribers, key, MapSet.new())
    new_subscribers = MapSet.put(subscribers, subscriber)
    new_state = put_in(state.subscribers[key], new_subscribers)
    {:reply, :ok, new_state}
  end

  def handle_call({:unsubscribe, key, subscriber}, _from, state) do
    subscribers = Map.get(state.subscribers, key, MapSet.new())
    new_subscribers = MapSet.delete(subscribers, subscriber)
    new_state = put_in(state.subscribers[key], new_subscribers)
    {:reply, :ok, new_state}
  end

  def handle_call({:set_nested, keys, value, ttl}, _from, state) when is_list(keys) do
    new_state = do_set_nested(keys, value, ttl, state)
    {:reply, :ok, new_state}
  end

  def handle_call({:get_nested, keys}, _from, state) when is_list(keys) do
    {result, new_state} = do_get_nested(keys, state)
    {:reply, result, new_state}
  end

  @impl true
  def handle_info(:save, state) do
    save_state(state)
    schedule_save()
    {:noreply, state}
  end

  def handle_info({:check_expiry, key}, state) do
    case Map.get(state.data, key) do
      nil ->
        {:noreply, state}
      {value, expiry} ->
        if expiry != :infinity and expiry <= current_time() do
          new_state = do_set(key, value, :expired, state)
          notify_subscribers(key, :expired, new_state)
          {:noreply, new_state}
        else
          {:noreply, state}
        end
    end
  end

  def handle_info({:check_expiry_nested, keys}, state) do
    case get_in(state.data, keys) do
      nil ->
        {:noreply, state}
      %{value: value, expiry: expiry} ->
        if expiry != :infinity and expiry <= current_time() do
          new_state = do_set_nested(keys, value, :expired, state)
          notify_subscribers(keys, :expired, new_state)
          {:noreply, new_state}
        else
          {:noreply, state}
        end
    end
  end

  # Private functions

  defp process_operation({:set, key, value, ttl}, state) do
    new_state = do_set(key, value, ttl, state)
    {:ok, new_state}
  end

  defp process_operation({:get, key}, state) do
    do_get(key, state)
  end

  defp process_operation({:delete, key}, state) do
    new_state = do_delete(key, state)
    {:ok, new_state}
  end

  defp do_set(key, value, ttl, state) do
    Logger.debug("Setting #{inspect(key)} to #{inspect(value)} with TTL #{inspect(ttl)}")

    expiry = calculate_expiry(ttl)
    new_data = Map.put(state.data, key, {value, expiry})

    timer_ref =
      if ttl != :infinity and ttl != :expired do
        Process.send_after(self(), {:check_expiry, key}, ttl)
      else
        nil
      end

    new_timers = Map.put(state.timers, key, timer_ref)
    new_state = %{state | data: new_data, timers: new_timers}
    save_state(new_state)

    notify_subscribers(key, {:set, value}, new_state)
    Logger.debug("New state: #{inspect(new_state)}")
    new_state
  end

  defp do_get(key, state) do
    Logger.debug("Getting #{inspect(key)}")

    case Map.get(state.data, key) do
      nil ->
        Logger.debug("#{inspect(key)} not found")
        {nil, state}

      {value, expiry} ->
        now = current_time()
        if expiry == :infinity or (expiry != :expired and expiry > now) do
          Logger.debug("#{inspect(key)} found with value #{inspect(value)}")
          {value, state}
        else
          Logger.debug("#{inspect(key)} expired")
          new_state = do_set(key, value, :expired, state)
          {nil, new_state}
        end
    end
  end

  defp do_delete(key, state) do
    Logger.debug("Deleting #{inspect(key)}")
    new_data = Map.delete(state.data, key)
    new_timers = Map.delete(state.timers, key)
    new_state = %{state | data: new_data, timers: new_timers}
    save_state(new_state)
    notify_subscribers(key, :deleted, new_state)
    new_state
  end

  defp do_set_nested(keys, value, ttl, state) do
    Logger.debug("Setting nested #{inspect(keys)} to #{inspect(value)} with TTL #{inspect(ttl)}")

    expiry = calculate_expiry(ttl)
    new_data = set_nested_recursive(state.data, keys, %{value: value, expiry: expiry})

    timer_ref =
      if ttl != :infinity and ttl != :expired do
        Process.send_after(self(), {:check_expiry_nested, keys}, ttl)
      else
        nil
      end

    new_timers = set_nested_recursive(state.timers, keys, timer_ref)
    new_state = %{state | data: new_data, timers: new_timers}
    save_state(new_state)

    notify_subscribers(keys, {:set, value}, new_state)
    Logger.debug("New state: #{inspect(new_state)}")
    new_state
  end

  defp set_nested_recursive(data, [key], value) do
    Map.put(data, key, value)
  end

  defp set_nested_recursive(data, [key | rest], value) do
    Map.put(data, key, set_nested_recursive(Map.get(data, key, %{}), rest, value))
  end

  defp do_get_nested(keys, state) do
    Logger.debug("Getting nested #{inspect(keys)}")

    case get_nested_recursive(state.data, keys) do
      nil ->
        Logger.debug("#{inspect(keys)} not found")
        {nil, state}

      %{value: value, expiry: expiry} ->
        now = current_time()
        if expiry == :infinity or (expiry != :expired and expiry > now) do
          Logger.debug("#{inspect(keys)} found with value #{inspect(value)}")
          {value, state}
        else
          Logger.debug("#{inspect(keys)} expired")
          new_state = do_set_nested(keys, value, :expired, state)
          {nil, new_state}
        end

      _ ->
        Logger.debug("#{inspect(keys)} not found or invalid format")
        {nil, state}
    end
  end

  defp get_nested_recursive(data, []) do
    data
  end

  defp get_nested_recursive(data, [key | rest]) when is_map(data) do
    case Map.get(data, key) do
      nil -> nil
      value -> get_nested_recursive(value, rest)
    end
  end

  defp get_nested_recursive(_, _) do
    nil
  end

  defp calculate_expiry(:infinity), do: :infinity
  defp calculate_expiry(:expired), do: :expired
  defp calculate_expiry(ttl) when is_integer(ttl), do: current_time() + ttl
  defp calculate_expiry(_), do: :expired  # Default case for invalid input

  defp notify_subscribers(keys, event, state) when is_list(keys) do
    subscribers = Map.get(state.subscribers, keys, MapSet.new())
    for subscriber <- subscribers do
      send(subscriber, {:kv_update, keys, event})
    end
  end
  defp notify_subscribers(key, event, state) when is_binary(key) or is_atom(key) do
    subscribers = Map.get(state.subscribers, key, MapSet.new())
    for subscriber <- subscribers do
      send(subscriber, {:kv_update, key, event})
    end
  end

  defp blank?(data) when is_map(data), do: Enum.empty?(data)
  defp blank?(str_or_nil), do: "" == str_or_nil |> to_string() |> String.trim()

  defp save_state(state) do
    case blank?(state.data) do
      true ->
        Logger.warning("No data to save")
        :ok

      false ->
        binary = :erlang.term_to_binary(state.data)
        compressed = :zlib.compress(binary)

        case File.write(state.persistence_file, compressed) do
          :ok ->
            Logger.debug("State saved successfully")
            :ok

          {:error, reason} ->
            Logger.error("Failed to save state: #{inspect(reason)}")
            {:error, reason}
        end
    end
  end

  defp load_state(persistence_file) do
    case File.read(persistence_file) do
      {:ok, compressed} ->
        try do
          binary = :zlib.uncompress(compressed)
          data = :erlang.binary_to_term(binary)
          now = current_time()

          valid_data = Enum.reduce(data, %{}, fn {key, {value, expiry}}, acc ->
            if expiry == :infinity or expiry > now do
              Map.put(acc, key, {value, expiry})
            else
              Logger.debug("Expired key removed during load: #{inspect(key)}")
              acc
            end
          end)

          %{data: valid_data, timers: %{}}
        rescue
          _ ->
            Logger.error("Failed to decompress or parse the state file. Starting with empty state.")
            %{data: %{}, timers: %{}}
        end

      {:error, :enoent} ->
        Logger.info("No existing state file found. Starting with empty state.")
        %{data: %{}, timers: %{}}

      {:error, reason} ->
        Logger.error("Failed to load state: #{inspect(reason)}")
        %{data: %{}, timers: %{}}
    end
  end

  defp current_time do
    System.system_time(:millisecond)
  end

  defp schedule_save do
    Process.send_after(self(), :save, @save_interval)
  end
end
