defmodule UUIDReplaceGenerator do
  def process(filename) do
    contents = filename |> File.read!
    modified_contents =
    ~r/(?<=\#)\{\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}\}/im
      |> Regex.replace(contents, fn -> "{" <> UUID.uuid4 <> "}" end, global: true)
    ext = filename |> Path.extname
    rootname = filename |> Path.rootname(ext)
    new_file_name = rootname <> "_modified" <> ext
    new_file_name |> File.write!(modified_contents)
    new_file_name
  end

  def modify(string) do
    ~r/(?<=\#)\{\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}\}/im
      |> Regex.replace(string, fn -> "{" <> UUID.uuid4 <> "}" end, global: true)
  end

  def extract_mms(filename) do
    contents = filename |> File.read!
    modified_contents =
    ~r/^\s*FIELD\s+\#MMS\_\w+[\w\W]*ENDPROPERTIES/imU
      |> Regex.scan(contents)
      |> Stream.map(&((hd(&1) <> "\r\n") |> modify))
      |> Enum.join("\r\n")
    filename
      |> modify_filename
      |> File.write!(modified_contents)
  end

  defp modify_filename(filename) do
    ext = filename |> Path.extname
    (filename |> Path.rootname(ext)) <> "_modified" <> ext
  end
end
