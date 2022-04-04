defmodule Sqlite.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: Sqlite.Worker.start_link(arg)
      # {Sqlite.Worker, arg}
      State,
      DatabaseSupervisor
      # {Task.Supervisor, name: Sqlite.DatabaseSupervisor},
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Sqlite.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
