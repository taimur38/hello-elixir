defmodule KV.Registry do
	use GenServer

	@doc """
	starts the registry

	`:name` is always required.
	"""
	def start_link(opts) do

		server = Keyword.fetch!(opts, :name)
		GenServer.start_link(__MODULE__, server, opts)
	end

	@doc """
	Looks up the bucket pid for name stored in server
	returns `{:ok, pid}` if the bucket exists, `:error` otherwise
	"""
	def lookup(server, name) do

		case :ets.lookup(server, name) do 
			[{^name, pid}] -> {:ok, pid}
			[] -> :error
		end
	end

	def create(server, name) do 
		GenServer.call(server, {:create, name})
	end


	def init(table) do 
		names = :ets.new(table, [:named_table, read_concurrency: true])
		refs = %{}
		{:ok, {names, refs}}
	end

	def handle_call({:create, name}, _from, {names, refs} = state) do 

		case lookup(names, name) do
			{:ok, pid} -> 
				{:reply, pid, state}
			:error ->
				{:ok, pid} = KV.BucketSupervisor.start_bucket()
				ref = Process.monitor(pid)
				refs = Map.put(refs, ref, name)
				:ets.insert(names, {name, pid})
				{:reply, pid, {names, refs}}
		end
	end

	def handle_info({:DOWN, ref, :process, _pid, _reason}, {names, refs}) do
		{name, refs} = Map.pop(refs, ref)
		:ets.delete(names, name)
		{:noreply, {names, refs}}
	end

	def handle_info(_msg, state) do
		{:noreply, state}
	end
end