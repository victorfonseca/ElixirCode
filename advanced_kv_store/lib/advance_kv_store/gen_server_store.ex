defmodule AdvancedKvStore.GenServerStore do
  use GenServer

  # Client API
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def set(pid, key, value) do
    GenServer.cast(pid, {:set, key, value})
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
  def handle_cast({:set, key, value}, state) do
    new_state = Map.put(state, key, value)
    IO.puts("New key added: #{key}")
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:delete, key}, state) do
    new_state = Map.delete(state, key)
    IO.puts("The key has been deleted: #{key}")
    {:noreply, new_state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state, key), state}
  end
end
