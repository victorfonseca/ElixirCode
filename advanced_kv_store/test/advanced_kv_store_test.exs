defmodule AdvancedKvStoreTest do
  use ExUnit.Case
  doctest AdvancedKvStore

  test "greets the world" do
    assert AdvancedKvStore.hello() == :world
  end
end
