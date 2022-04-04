defmodule DatabaseSupervisor do
    use DynamicSupervisor

    def start_link(init_args) do
      DynamicSupervisor.start_link(__MODULE__, init_args, name: __MODULE__)
    end

    def init(_init_args) do
      DynamicSupervisor.init(strategy: :one_for_one)
    end

    def start_child(name) do
      spec = {Database, name: name}
      DynamicSupervisor.start_child(__MODULE__, spec)
      IO.puts("#{name}")
    end
    # def stop_child(count) do
    #   # spec = {Database, name: name}
    #   name = String.to_atom("user#{count}")
    # pid =   Process.whereis(name)
    #   DynamicSupervisor.terminate_child(__MODULE__, pid)
    #   IO.puts("#{name}")
    # end


    def db_maker(start_count, end_count) when start_count > end_count do
      Task.async(fn -> start_child(:report)end)
      |> Task.await()
    end
    def db_maker(start_count, end_count) do
      count = end_count -  start_count
    Task.async(fn -> start_child(:"user#{start_count}")end)
      # [max_concurrency: count]) # Added the timeout here
    |> Task.await()
    db_maker(start_count + 1, end_count)
    end
end
