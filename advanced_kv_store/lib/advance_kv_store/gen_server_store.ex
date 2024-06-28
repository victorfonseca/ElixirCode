defmodule AdvancedKvStore.GenServerStore do
  use GenServer
  require Logger

  @persistence_file "kv_store.dat"
  @save_interval :timer.minutes(1)

  defp calculate_expiry(:infinity), do: :infinity
  defp calculate_expiry(ttl), do: System.system_time(:millisecond) + ttl

  # Client API
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def set(pid, key, value, ttl \\ :infinity) do
    GenServer.call(pid, {:set, key, value, ttl})
  end

  def update_ttl(pid, key, new_ttl) do
    GenServer.call(pid, {:update_ttl, key, new_ttl})
  end

  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  def delete(pid, key) do
    GenServer.call(pid, {:delete, key})
  end

  def list_keys(pid) do
    GenServer.call(pid, :list_keys)
  end

  def list_values(pid) do
    GenServer.call(pid, :list_values)
  end

  defp save_state(state) do
    binary = :erlang.term_to_binary(state.data)
    File.write!(@persistence_file, binary)
  end

  defp load_state do
    case File.read(@persistence_file) do
      {:ok, binary} ->
        data = :erlang.binary_to_term(binary)
        %{data: data, timers: %{}}
      {:error, :enoent} ->
        %{data: %{}, timers: %{}}
    end
  end

  # Server Callbacks
  @impl true
  def init(:ok) do
    Logger.debug("GenServerStore initialized")
    state = load_state()
    schedule_save()
    {:ok, state}
  end

  @impl true
  def handle_info(:save, state) do
    Logger.debug("Saving state to disk")
    save_state(state)
    schedule_save()
    {:noreply, state}
  end

  @impl true
  def handle_info({:check_expiry, key}, state) do
    Logger.debug("Checking expiry for #{inspect(key)}")
    case Map.get(state.data, key) do
      nil ->
        Logger.debug("#{inspect(key)} not found")
        {:noreply, state}
      {_value, expiry} ->
        current_time = System.system_time(:millisecond)
        if expiry == :infinity or expiry > current_time do
          Logger.debug("#{inspect(key)} not expired yet")
          time_left = max(0, expiry - current_time)
          new_timer = Process.send_after(self(), {:check_expiry, key}, time_left)
          new_timers = Map.put(state.timers, key, new_timer)
          new_state = %{state | timers: new_timers}
          {:noreply, new_state}
        else
          Logger.debug("#{inspect(key)} expired and removed")
          new_data = Map.delete(state.data, key)
          new_timers = Map.delete(state.timers, key)
          new_state = %{state | data: new_data, timers: new_timers}
          {:noreply, new_state}
        end
    end
  end

  defp schedule_save do
    Process.send_after(self(), :save, @save_interval)
  end

  @impl true
  def handle_call({:set, key, value, ttl}, _from, state) do
    Logger.debug("Setting #{inspect(key)} to #{inspect(value)} with TTL #{inspect(ttl)}")

    expiry = calculate_expiry(ttl)
    new_data = Map.put(state.data, key, {value, expiry})

    timer_ref =
      if ttl != :infinity do
        Process.send_after(self(), {:check_expiry, key}, ttl)
      else
        nil
      end

    new_timers = Map.put(state.timers, key, timer_ref)
    new_state = %{state | data: new_data, timers: new_timers}
    save_state(new_state)

    Logger.debug("New state: #{inspect(new_state)}")
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:update_ttl, key, new_ttl}, _from, state) do
    Logger.debug("Updating TTL for #{inspect(key)} to #{inspect(new_ttl)}")

    case Map.get(state.data, key) do
      nil ->
        Logger.debug("#{inspect(key)} not found")
        {:reply, {:error, :not_found}, state}

      {value, old_expiry} ->
        current_time = System.system_time(:millisecond)
        if old_expiry != :infinity and old_expiry <= current_time do
          Logger.debug("#{inspect(key)} has already expired")
          new_data = Map.delete(state.data, key)
          new_timers = Map.delete(state.timers, key)
          new_state = %{state | data: new_data, timers: new_timers}
          {:reply, {:error, :expired}, new_state}
        else
          new_expiry = calculate_expiry(new_ttl)
          new_data = Map.put(state.data, key, {value, new_expiry})

          # Cancel the existing timer if there is one
          if old_expiry != :infinity do
            Process.cancel_timer(Map.get(state.timers, key, nil))
          end

          # Set a new timer if the new TTL is not :infinity
          new_timer =
            if new_ttl != :infinity do
              Process.send_after(self(), {:check_expiry, key}, new_ttl)
            else
              nil
            end

          # Update the timers map
          new_timers = Map.put(state.timers, key, new_timer)
          new_state = %{state | data: new_data, timers: new_timers}
          save_state(new_state)

          Logger.debug("New TTL: #{inspect(new_expiry)}")
          {:reply, :ok, new_state}
        end
    end
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    Logger.debug("Getting #{inspect(key)}")
    case Map.get(state.data, key) do
      nil ->
        Logger.debug("#{inspect(key)} not found")
        {:reply, nil, state}
      {value, expiry} ->
        current_time = System.system_time(:millisecond)
        if expiry == :infinity or expiry > current_time do
          Logger.debug("#{inspect(key)} found with value #{inspect(value)}")
          {:reply, value, state}
        else
          Logger.debug("#{inspect(key)} expired")
          new_data = Map.delete(state.data, key)
          new_timers = Map.delete(state.timers, key)
          new_state = %{state | data: new_data, timers: new_timers}
          {:reply, nil, new_state}
        end
    end
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    Logger.debug("Deleting #{inspect(key)}")
    new_data = Map.delete(state.data, key)
    new_timers = Map.delete(state.timers, key)
    new_state = %{state | data: new_data, timers: new_timers}
    save_state(new_state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:list_keys, _from, state) do
    Logger.debug("Listing all keys")
    keys = Map.keys(state.data)
    {:reply, keys, state}
  end

  @impl true
  def handle_call(:list_values, _from, state) do
    Logger.debug("Listing all values")
    values = state.data
    |> Map.values()
    |> Enum.map(fn {value, expiry} ->
      current_time = System.system_time(:millisecond)
      if expiry == :infinity or expiry > current_time do
        value
      else
        nil
      end
    end)
    |> Enum.reject(&is_nil/1)
    {:reply, values, state}
  end
end
