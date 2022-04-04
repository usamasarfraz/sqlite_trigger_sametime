defmodule State do
    use GenServer
    def start_link(args) do
        GenServer.start_link(__MODULE__,%{}, name: __MODULE__)
    end

    def insert(data) do
        GenServer.cast(__MODULE__, {:insert_data, data})
    end

    def get() do
        GenServer.call(__MODULE__, {:get_data})
    end


    def init(state) do
        {:ok, state}
    end

    def handle_cast({:insert_data, data}, _from, state) do
        {:reply, state, state}
    end

    def handle_call({:get_data}, _from, state) do
        {:reply, state, state}
    end
end