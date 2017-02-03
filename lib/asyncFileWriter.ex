defmodule AsyncFileWriter do
  use GenServer

  # API
  def start_link(filename) do
    GenServer.start_link(__MODULE__, [filename], name: __MODULE__)
  end

  def write(text) do
    __MODULE__ |> GenServer.cast({:write, text})
  end

  def close do
    __MODULE__ |> GenServer.call(:close)
  end

  # Callbacks
  def init(filename) do
    handle = filename |> File.open!([:write, :append, :utf8])
    {:ok, handle}
  end

  def handle_cast({:write, text}, file_handle) do
    file_handle |> IO.puts(text <> "\r\n")
    {:noreply, file_handle}
  end

  def handle_call(:close, _from, file_handle) do
    result = file_handle |> File.close
    {:reply, result, file_handle}
  end
end
