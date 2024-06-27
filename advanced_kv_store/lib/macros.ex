defmodule AdvancedKvStore.Macros do
  defmacro while(condition, do: block) do
    quote do
      try do
        for _ <- Stream.cycle([:ok]) do
          if unquote(condition) do
            unquote(block)
          else
            throw :break
          end
        end
      catch
        :break -> :ok
      end
    end
  end
end
