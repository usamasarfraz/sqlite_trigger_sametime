defmodule SqliteTest do
  use ExUnit.Case
  doctest Sqlite

  test "greets the world" do
    assert Sqlite.hello() == :world
  end
end
