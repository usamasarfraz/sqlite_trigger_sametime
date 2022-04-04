defmodule Database do
  use GenServer, restart: :temporary
  alias Exqlite.{Connection, Sqlite3}
  # Public API

  @spec start_link(keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(args) do
    name = Keyword.get(args, :name)
    # db_name = Keyword.get(args, :db_name)
    GenServer.start_link(__MODULE__,%{}, name: name)
  end


#only dbs
#   def init(_arg) do
#     {:registered_name, name} = Process.info(self(), :registered_name)
#   File.mkdir_p("priv")
#      db_name = Atom.to_string(name)
#   case Connection.connect([database: "./priv/#{db_name}.db", journal_mode: :wal]) do
#     {:ok, database} -> :ok
#     {:error, reason} -> IO.puts(reason.message)
#       _ -> IO.puts("Something is wrong")
#   end
#     {:ok, name}
#   end



#   def create_tables(count) when count < 1, do: IO.puts("Tables created")
#   def create_tables(count) do
#     {:ok, database} = Exqlite.Sqlite3.open("./priv/user#{count}.db")


#    Sqlite3.execute(database, "create table tb1 (id integer primary key, stuff text, timestemp text)")
#     #  Sqlite3.execute(database, "create table tb2 (id integer primary key, stuff text, timestemp text)")
#     # Sqlite3.execute(database, "create table tb3 (id integer primary key, stuff text, timestemp text)")
#    IO.puts("count#{count}")

#     create_tables( count -  1)
#   end

#   def insert_data(count) when count < 1, do: :ok

#   def insert_data(count) do
#     name = "user#{count}"
#    IO.puts("#{name}")

#     {:ok, database} = Sqlite3.open("./priv/#{name}.db")
#     insert(1,database,name )
#     insert_data(count - 1)
#   end
#   def insert(tb_count,_db_id, _name) when tb_count < 1, do: :ok

#   def insert(tb_count, db_id, name) do

#     case Sqlite3.prepare(db_id, "insert into tb#{tb_count} (stuff, timestemp) values (?1, ?2)") do
#     {:ok, statement} ->  insert_into_table(50,statement, name,tb_count,db_id)
#     _ -> IO.puts("somthing is wrong is insert")
#     end
#   #  IO.puts("count#{tb_count}")

#     insert(tb_count - 1,db_id, name)
#   end

# defp insert_into_table(count, _statement,_name,_tb_count,_db_id) when count < 1, do: :ok
# defp insert_into_table(count, statement, name,tb_count,db_id) do
#   # IO.puts("insert#{count}_#{tb_count}_#{name}")
#   time = "Ali_#{Time.utc_now()}"
#   data = "#{name}_tb#{tb_count}_#{count}"
#   case Sqlite3.bind(db_id, statement, [data,time]) do
#   :ok -> Sqlite3.step(db_id, statement)
#   _ -> IO.puts("somthing is wrong is insert table")
#   end

#   insert_into_table(count - 1, statement,name,tb_count,db_id)
# end


# dbs with tables


def init(state) do
  {:registered_name, name} = Process.info(self(), :registered_name)
   db_name = Atom.to_string(name)

  # Task.Supervisor.async_nolink(Sqlite.DatabaseSupervisor, fn ->
  #   {:ok, database} =  Connection.connect([database: "./with_tb_priv/#{db_name}.db", journal_mode: :wal])
  #   {:set_state, :e, database.db}
  # end)

  # Task.async(fn ->
    {:ok, disk_database} =  Connection.connect([database: "./with_tb_priv/#{db_name}.db", journal_mode: :wal])
    {:ok, memory_database} =  Connection.connect([database: ":memory:", journal_mode: :memory])
    get_from_disk_to_memory(state, disk_database.db, memory_database.db)
    {:set_state,  %{memory_conn: memory_database.db , disk_conn: disk_database.db}}
    # {:set_state,  %{memory_conn: memory_database.db}}
  # end)

  # |> Task.await()

  # Task.Supervisor.async_nolink(Sqlite.DatabaseSupervisor, fn ->
  #   {:ok, database} =  Connection.connect([database: "g:/with_tb_priv/#{db_name}.db", journal_mode: :wal])
  #   {:set_state, :g, database.db}
  # end)

  # Task.Supervisor.async_nolink(Sqlite.DatabaseSupervisor, fn ->
  #   {:ok, database} =  Connection.connect([database: "h:/with_tb_priv/#{db_name}.db", journal_mode: :wal])
  #   {:set_state, :h, database.db}
  # end)

#    Task.async(fn ->
#     File.mkdir_p("./with_tb_priv")
#    case Connection.connect([database: "./with_tb_priv/#{db_name}.db", journal_mode: :wal]) do
#      {:ok, database} -> create_tables(1,database.db, name)
#      {:error, reason} -> IO.puts(reason.message)
#        _ -> IO.puts("Something is wrong")
#    end
#  end)
#  Task.async(fn ->
#  File.mkdir_p("e:/with_tb_priv")
# case Connection.connect([database: "e:/with_tb_priv/#{db_name}.db", journal_mode: :wal]) do
#   # {:ok, database} -> create_tables(1,database.db, name)
#   {:ok, database} -> Map.put(state, :e, database)
#   {:error, reason} -> IO.puts(reason.message)
#     _ -> IO.puts("Something is wrong")
# end


# end)
# Task.async(fn ->
#   File.mkdir_p("f:/with_tb_priv")

#   case Connection.connect([database: "f:/with_tb_priv/#{db_name}.db", journal_mode: :wal]) do
#     {:ok, database} ->  Map.put(state, :f, database)
#     {:error, reason} -> IO.puts(reason.message)
#       _ -> IO.puts("Something is wrong")
#   end
# end)
# Task.async(fn ->
#   File.mkdir_p("g:/with_tb_priv")

#   case Connection.connect([database: "g:/with_tb_priv/#{db_name}.db", journal_mode: :wal]) do
#     {:ok, database} ->  Map.put(state, :g, database)
#     {:error, reason} -> IO.puts(reason.message)
#       _ -> IO.puts("Something is wrong")
#   end
# end)

# Task.async(fn ->
#   File.mkdir_p("h:/with_tb_priv")

#   case Connection.connect([database: "h:/with_tb_priv/#{db_name}.db", journal_mode: :wal]) do
#     {:ok, database} ->  Map.put(state, :h, database)
#     {:error, reason} -> IO.puts(reason.message)
#       _ -> IO.puts("Something is wrong")
#   end
# end)

# use ets

  {:ok, %{memory_conn: memory_database.db , disk_conn: disk_database.db}}
end

def get_report(db_start,db_end,record_start,record_end) do
  db_start..db_end
    |> Stream.map(fn counter -> Task.async(fn -> GenServer.call( String.to_atom("user#{counter}"), {:get_report, counter, record_start, record_end})end)end)
    # [max_concurrency: count, timeout: 30_000]) # Added the timeout here end)
    # |> Enum.map(fn result -> result end)
    # |> IO.inspect()
    |> Enum.map(&Task.await(&1))
end

def get_from_memory(user) do
  GenServer.call(String.to_atom(user), {:get_record, String.to_atom("memory_conn")})
  :ok
end

def get_from_disk(user) do
  GenServer.call(String.to_atom(user), {:get_record, String.to_atom("disk_conn")})
  :ok
end

def get_column_names(disk_database, table_name) do
  {:ok, statement} = Exqlite.Sqlite3.prepare(disk_database, "select name, type from pragma_table_info('#{table_name}')")
  {:done, col_list} = Exqlite.Sqlite3.multi_step(disk_database,statement)
  col_list
end

def table_query_maker(tb_name, col_list) do
  query = "create table #{tb_name} (id integer primary key"
  query = Enum.reduce(col_list, query, fn([name,type], acc)-> "#{acc}, #{name} #{type}" end)
  # for [name , type] <- col_list do
  #   query = "#{query}, #{name} #{type}"
  # end
  "#{query})"
end

def get_from_disk_to_memory(state, disk_conn, memory_conn) do
  case get_data(disk_conn, "tb1") do
    {:ok, tb1_data} -> insert_in_memory(disk_conn, memory_conn, "tb1", tb1_data, "firstname", "lastname", "age")
    err -> IO.puts("table tb1 not existed on disk")
  end

  case get_data(disk_conn, "tb2") do
    {:ok, tb2_data} -> insert_in_memory(disk_conn, memory_conn, "tb2", tb2_data, "productcode", "name", "colour")
    err -> IO.puts("table tb2 not existed on disk")
  end

  :ok
end

def insert_in_memory(disk_conn, memory_conn, tb_name, data, field1, field2, field3) do
  [_ | col_list] = get_column_names(disk_conn, tb_name)
  query = table_query_maker(tb_name, col_list)
  Sqlite3.execute(memory_conn, query)
  for [_ | data_list] <- data do
    insert_data_in_memory(memory_conn, data_list, tb_name, field1, field2, field3)
  end
end

def create_tables(start_count,end_count) do
  count = end_count -  start_count
    start_count..end_count
    |> Stream.map(fn counter -> Task.async(fn -> GenServer.call( String.to_atom("user#{counter}"), {:create_tb,  String.to_atom("user#{counter}")})end)end)
    # [max_concurrency: count, timeout: 30_000]) # Added the timeout here end)
    # |> Enum.map(fn result -> result end)
    # |> IO.inspect()
    |> Enum.map(&Task.await(&1))
    GenServer.call( :report, {:create_report_tb})
end

def add_column(counter, table_name, column, type) do
  GenServer.cast( String.to_atom("user#{counter}"), {:add_column, table_name, column, type})
end

#callbacks

def handle_call({:create_tb, name}, _from, state) do
  # Task.async(fn ->
    disk_database =  Map.fetch!(state, :disk_conn)
    # memory_database =  Map.fetch!(state, :memory_conn)
    Sqlite3.execute(disk_database, "create table tb1 (id integer primary key, firstname text, lastname text, age text, timestemp text, ack text)")
    Sqlite3.execute(disk_database, "create table tb2 (id integer primary key, productcode text, name text, colour text, timestemp text, ack text)")
    # Sqlite3.execute(memory_database, "create table tb1 (id integer primary key, firstname text, lastname text, age text, timestemp text, ack text)")
    # Sqlite3.execute(memory_database, "create table tb2 (id integer primary key, productcode text, name text, colour text, timestemp text, ack text)")
    # Sqlite3.execute(memory_database, "create table report (id integer primary key, db_name text, db_name_number integer, first_record_number integer, second_record_number integer, req_timestmp_string text, ack_timestmp_string text, req_timestmp text, ack_timestmp text, net_time_miliseconds integer)")
  # end)
  # |> Task.await()
  {:reply, state, state}
end

def handle_call({:create_report_tb}, _from, state) do
  disk_database =  Map.fetch!(state, :disk_conn)
  Sqlite3.execute(disk_database, "create table report (id integer primary key, db_name text, db_name_number integer, first_record_number integer, second_record_number integer, req_timestmp_string text, ack_timestmp_string text, req_timestmp text, ack_timestmp text, net_time_miliseconds integer)")
  {:reply, state, state}
end

def handle_call({:get_report, counter, rec_start, rec_end}, _from, state) do
  conn = Map.fetch!(state, String.to_atom("disk_conn"))
  {:ok, statement} = Exqlite.Sqlite3.prepare(conn, "SELECT id,timestemp,ack FROM tb1 where id between #{rec_start} and #{rec_end}");
  {:ok, [[id1,timeStamp,_],[id2,_,ack]]} =  Exqlite.Sqlite3.fetch_all(conn,statement)

  {:ok, ack_time_format} = Time.from_iso8601(ack)
  {:ok, timeStamp_time_format} = Time.from_iso8601(timeStamp)
  time_diff = Time.diff(ack_time_format, timeStamp_time_format, :millisecond)

  GenServer.call(:report, {:add_report,  "user_#{counter}", counter, id1, id2, "user_#{timeStamp}", "user_#{ack}", timeStamp, ack, time_diff})


  {:reply, state, state}
end

def handle_call({:add_report, db_name, counter, id1, id2, time_string, ack_string, timeStamp, ack, time_diff}, _from, state) do
  conn = Map.fetch!(state, String.to_atom("disk_conn"))
  insert_in_report_table(conn, db_name, counter, id1, id2, time_string, ack_string, timeStamp, ack, time_diff)
  {:reply, state, state}
end

def handle_cast({:add_column, table_name, column, type},state) do
  disk_database =  Map.fetch!(state, :disk_conn)
  memory_database =  Map.fetch!(state, :memory_conn)
  Sqlite3.execute(disk_database, "alter table #{table_name} add column #{column} #{type}")
  Sqlite3.execute(memory_database, "alter table #{table_name} add column #{column} #{type}")
  {:noreply, state}
end

def handle_info({_ref, {:set_state, data}}, state) do
  {:noreply, Map.merge(state, data)}
end


@doc false
def handle_info({_pid, _payload}, state),
do: {:noreply, state}

@doc false
def handle_info({:DOWN, _ref, :process, _pid, :normal}, state),
do: {:noreply, state}




def insert_data(start_count, end_count, target_hour, target_minute) do

   count = end_count -  start_count
   start_count..end_count
    |> Task.async_stream(fn counter -> GenServer.cast( String.to_atom("user#{counter}"), {:add_record, String.to_atom("user#{counter}"), counter, target_hour, target_minute})end,
    [max_concurrency: count, timeout: 5000000]) # Added the timeout here
    # end)
    |> Enum.map(fn ({ :ok, result }) -> result end)
    # |> Task.await_many()
end
def handle_cast({:add_record, name, count, target_hour, target_minute}, state) do
    {:ok, target_time} = Time.new(target_hour,target_minute,10,307000)

    # Task.async(fn ->
      disk_database = Map.fetch!(state, :disk_conn)
      # memory_database = Map.fetch!(state, :memory_conn)
      current_time = Time.utc_now()
      time_diff = Time.diff(target_time, current_time, :millisecond)
      Process.sleep(time_diff)
    # time = "Ali_#{Time.utc_now()}"
    # IO.inspect(time)
    insert(2,disk_database,name,count)
    # insert(2,memory_database,name,count)
  # end)
  # |> Task.await()
  # Task.Supervisor.async_nolink(Sqlite.DatabaseSupervisor, fn ->
  #   database = Map.fetch!(state, :f)
  #   insert(1,database,name)
  # end)
  # Task.Supervisor.async_nolink(Sqlite.DatabaseSupervisor, fn ->
  #   database = Map.fetch!(state, :g)
  #   insert(1,database,name)
  # end)
  # Task.Supervisor.async_nolink(Sqlite.DatabaseSupervisor, fn ->
  #   database = Map.fetch!(state, :h)
  #   insert(1,database,name)
  # end)

  {:noreply, state}
end




def insert(tb_count,_db_id, _name,_count, _prev_tb1_ack, _prev_tb2_ack) when tb_count < 1, do: :ok

def insert(tb_count, db_id, name, count, prev_tb1_ack, prev_tb2_ack) do

  case Sqlite3.prepare(db_id, "insert into tb1 (firstname, lastname, age, timestemp, ack) values (?1, ?2, ?3, ?4, ?5)") do
  {:ok, statement} ->  insert_into_table(tb_count, statement, name, tb_count, db_id, prev_tb1_ack)
  end
  tb1_ack = Time.utc_now()
  # case Sqlite3.prepare(db_id, "insert into tb2 (productcode, name, colour, timestemp, ack) values (?1, ?2, ?3, ?4, ?5)") do
  # {:ok, statement} ->  insert_into_table(tb_count, statement, name, tb_count, db_id, prev_tb2_ack)
  # end
  tb2_ack = Time.utc_now()

insert(tb_count - 1, db_id, name, count, tb1_ack, tb2_ack)
end

def insert(tb_count, db_id, name,count) do

  case Sqlite3.prepare(db_id, "insert into tb1 (firstname, lastname, age, timestemp, ack) values (?1, ?2, ?3, ?4, ?5)") do
  {:ok, statement} ->  insert_into_table(tb_count, statement, name, tb_count, db_id, "")
  end
  tb1_ack = Time.utc_now()
  # case Sqlite3.prepare(db_id, "insert into tb2 (productcode, name, colour, timestemp, ack) values (?1, ?2, ?3, ?4, ?5)") do
  # {:ok, statement} ->  insert_into_table(tb_count, statement, name, tb_count, db_id, "")
  # end
  tb2_ack = Time.utc_now()
insert(tb_count - 1, db_id, name, count, tb1_ack, tb2_ack)
end

defp insert_into_table(count, _statement,_name,_tb_count,_db_id, _ack) when count < 1, do: :ok
defp insert_into_table(count, statement, name, tb_count, db_id, ack) do
# IO.puts("insert#{count}_#{tb_count}_#{name}")
time = Time.utc_now()
  IO.inspect(time)
case Sqlite3.bind(db_id, statement, [count, count, count, time, ack]) do
:ok -> Sqlite3.step(db_id, statement)
_ -> IO.puts("somthing is wrong in insert table")
end

# insert_into_table(count - 1, statement, name, tb_count, db_id, "")
end

def insert_in_report_table(db_id, db_name, db_name_number, first_record_number, second_record_number, req_timestmp_string, ack_timestmp_string, req_timestmp, ack_timestmp, net_time_miliseconds) do
  case Sqlite3.prepare(db_id, "insert into report (db_name, db_name_number, first_record_number, second_record_number, req_timestmp_string, ack_timestmp_string, req_timestmp, ack_timestmp, net_time_miliseconds) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)") do
    {:ok, statement} ->  case Sqlite3.bind(db_id, statement, [db_name, db_name_number, first_record_number, second_record_number, req_timestmp_string, ack_timestmp_string, req_timestmp, ack_timestmp, net_time_miliseconds]) do
      :ok -> Sqlite3.step(db_id, statement)
      _ -> IO.puts("somthing is wrong in insert table")
      end
    end
end

def insert_data_in_memory(db_conn, data_list, tb_name, field1, field2, field3) do
case Sqlite3.prepare(db_conn, "insert into #{tb_name} (#{field1}, #{field2}, #{field3}, timestemp, ack) values (?1, ?2, ?3, ?4, ?5)") do
  {:ok, statement} ->  case Sqlite3.bind(db_conn, statement, data_list) do
    :ok -> Sqlite3.step(db_conn, statement)
    reson -> IO.inspect(reson)
    end
end
end

def get_data(conn, table_name) do
case Exqlite.Sqlite3.prepare(conn, "SELECT * FROM #{table_name}") do
  {:ok, statement} -> Exqlite.Sqlite3.fetch_all(conn,statement)
  {:error, reason} -> reason
end
end
def handle_call({:get_record, conn_name}, _from, state) do
conn = Map.fetch!(state, conn_name)
{:ok, statement} = Exqlite.Sqlite3.prepare(conn, "SELECT * FROM tb1");
# data = Exqlite.Sqlite3.step(conn, statement)
data =  Exqlite.Sqlite3.fetch_all(conn,statement)
IO.puts("-------------data-------------")
IO.inspect(data)
IO.puts("-------------data-------------")
{:reply,data,state}
end

def stop(start_count, end_count) do
count = end_count -  start_count
Task.async_stream(start_count..end_count,fn counter ->
   GenServer.stop(String.to_atom("user#{counter}"))
   end,
[max_concurrency: 1, timeout: 30_000]) # Added the timeout here
|> Task.await_many()
# GenServer.stop(String.to_atom("user#{counter}"),:normal,:infinity)
end
def terminate(_reason,  state) do
memory_conn = Map.fetch!(state, :memory_conn)
disk_conn = Map.fetch!(state, :disk_conn)
# # conn = Map.fetch!(state, name)
Sqlite3.close(memory_conn)
Sqlite3.close(disk_conn)
:ok
end

end
