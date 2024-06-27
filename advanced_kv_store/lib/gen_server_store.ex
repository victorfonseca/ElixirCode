defmodule AdvancedKvStore.GenServerStore do
  use GenServer

  # Client API
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def set(pid, key, value, ttl \\ :infinity) do
    GenServer.cast(pid, {:set, key, value, ttl})
  end

  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  def delete(pid, key) do
    GenServer.cast(pid, {:delete, key})
  end

  # Server Callbacks
  @impl true
  def init(:ok) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:set, key, value, ttl}, state) do
    if ttl != :infinity do
      Process.send_after(self(), {:expire, key}, ttl)
    end
    {:noreply, Map.put(state, key, value)}
  end

  @impl true
  def handle_cast({:delete, key}, state) do
    {:noreply, Map.delete(state, key)}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state, key), state}
  end

  @impl true
  def handle_info({:expire, key}, state) do
    {:noreply, Map.delete(state, key)}
  end
end
