defmodule AdvancedKvStore.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.debug("Starting AdvancedKvStore application")
    children = [
      {AdvancedKvStore.GenServerStore, name: AdvancedKvStore.KVStore}
    ]

    opts = [strategy: :one_for_one, name: AdvancedKvStore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
