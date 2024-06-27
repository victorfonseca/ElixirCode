defmodule AdvancedKvStoreTest do
  use ExUnit.Case
  doctest AdvancedKvStore

  alias AdvancedKvStore.Macros
  alias AdvancedKvStore.Protocols
  alias AdvancedKvStore.GenServerStore

  test "while macro" do
    import Macros
    x = 0
    while x < 5 do
      x = x + 1
    end
    assert x == 5
  end

  test "Serializable protocol" do
    assert Protocols.Serializable.serialize([1, 2, 3]) == "[1,2,3]"
    assert Protocols.Serializable.serialize(%{a: 1, b: 2}) == "{a:1,b:2}"
    custom_struct = %Protocols.CustomStruct{name: "test", value: 42}
    assert Protocols.Serializable.serialize(custom_struct) == "CustomStruct(name:test,value:42)"
  end

  test "GenServerStore" do
    {:ok, pid} = GenServerStore.start_link()
    GenServerStore.set(pid, :key1, "value1")
    assert GenServerStore.get(pid, :key1) == "value1"
    GenServerStore.delete(pid, :key1)
    assert GenServerStore.get(pid, :key1) == nil
    GenServerStore.set(pid, :key2, "value2", 100)
    assert GenServerStore.get(pid, :key2) == "value2"
    :timer.sleep(150)
    assert GenServerStore.get(pid, :key2) == nil
  end
end
