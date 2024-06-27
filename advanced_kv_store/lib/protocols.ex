defmodule AdvancedKvStore.Protocols do
  defprotocol Serializable do
    @doc "Converts the data structure to a string representation"
    def serialize(data)
  end

  defimpl Serializable, for: List do
    def serialize(list), do: "[#{Enum.join(list, ",")}]"
  end

  defimpl Serializable, for: Map do
    def serialize(map) do
      map
      |> Enum.map(fn {k, v} -> "#{k}:#{v}" end)
      |> Enum.join(",")
      |> (fn s -> "{#{s}}" end).()
    end
  end

  defmodule CustomStruct do
    defstruct [:name, :value]
  end

  defimpl Serializable, for: CustomStruct do
    def serialize(%CustomStruct{name: name, value: value}) do
      "CustomStruct(name:#{name},value:#{value})"
    end
  end
end
