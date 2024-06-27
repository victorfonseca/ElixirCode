defmodule AdvancedKvStore.GenServerStore do
  use GenServer
  require Logger

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

  # Server Callbacks
  @impl true
  def init(:ok) do
    Logger.debug("GenServerStore initialized")
    {:ok, %{}}
  end

  @impl true
  def handle_call({:set, key, value, ttl}, _from, state) do
    Logger.debug("Setting #{inspect(key)} to #{inspect(value)} with TTL #{inspect(ttl)}")

    expiry = calculate_expiry(ttl)
    new_state = Map.put(state, key, {value, expiry})

    if ttl != :infinity do
      Process.send_after(self(), {:check_expiry, key}, ttl)
    end

    Logger.debug("New state: #{inspect(new_state)}")
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:update_ttl, key, new_ttl}, _from, state) do
    Logger.debug("Updating TTL for #{inspect(key)} to #{inspect(new_ttl)}")

    case Map.get(state, key) do
      nil ->
        Logger.debug("#{inspect(key)} not found")
        {:reply, {:error, :not_found}, state}

      {value, _old_expiry} ->
        new_expiry = calculate_expiry(new_ttl)
        new_state = Map.put(state, key, {value, new_expiry})

        if new_ttl != :infinity do
          Process.send_after(self(), {:check_expiry, key}, new_ttl)
        end

        Logger.debug("New TTL: #{inspect(new_expiry)}")
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    Logger.debug("Getting #{inspect(key)}")
    case Map.get(state, key) do
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
          new_state = Map.delete(state, key)
          {:reply, nil, new_state}
        end
    end
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    Logger.debug("Deleting #{inspect(key)}")
    {:reply, :ok, Map.delete(state, key)}
  end

  @impl true
  def handle_call(:list_keys, _from, state) do
    Logger.debug("Listing all keys")
    keys = Map.keys(state)
    {:reply, keys, state}
  end

  @impl true
  def handle_call(:list_values, _from, state) do
    Logger.debug("Listing all values")
    values = state
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

  @impl true
  def handle_info({:check_expiry, key}, state) do
    Logger.debug("Checking expiry for #{inspect(key)}")
    case Map.get(state, key) do
      nil ->
        Logger.debug("#{inspect(key)} not found")
        {:noreply, state}
      {_value, expiry} ->
        current_time = System.system_time(:millisecond)
        if expiry == :infinity or expiry > current_time do
          Logger.debug("#{inspect(key)} not expired yet")
          time_left = max(0, expiry - current_time)
          Process.send_after(self(), {:check_expiry, key}, time_left)
          {:noreply, state}
        else
          Logger.debug("#{inspect(key)} expired and removed")
          {:noreply, Map.delete(state, key)}
        end
    end
  end
end
