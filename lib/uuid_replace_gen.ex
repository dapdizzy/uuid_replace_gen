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

  def extract(string, regex) do
    regex
      |> Regex.scan(string)
      |> Stream.map(&(hd(&1) <> "\r\n\r\n"))
      |> Enum.join
  end

  defp extract_transform(regex_string, source, transformation \\ &(&1)) do
    regex_string
      |> Regex.compile!([:caseless, :multiline, :ungreedy])
      |> Regex.scan(source)
      |> Stream.map(&((hd(&1) <> "\r\n\r\n") |> transformation))
      |> Enum.join
  end

  def transform(source_file, dest_file, prefix \\ "mms") do
    source = source_file |> File.read!
    fields =
      (~S|\s*FIELD\s+\#| <> prefix <> ~S|\w+[\w\W]*ENDPROPERTIES|)
      |> Regex.compile!([:caseless, :multiline, :ungreedy])
      |> Regex.scan(source)
      |> Stream.map(&((hd(&1) <> "\r\n\r\n") |> modify))
      |> Enum.join
    field_groups =
      (~S|\s*GROUP\s+\#| <> prefix <> ~S|\w+[\s\W]*ENDGROUP|)
      |> extract_transform(source)
    indexes =
      (~S|^\s*\#| <> prefix <> ~S|\s*$[\w\W]*ENDPROPERTIES|)
      |> extract_transform(source)
    methods =
      (~S|\s*SOURCE\s+\#| <> prefix <> ~S|\w+[\w\W]*ENDSOURCE|)
      |> Regex.compile!([:caseless, :multiline, :ungreedy])
      |> Regex.scan(source)
      |> Stream.map(&(hd(&1) <> "\r\n\r\n"))
      |> Enum.join
    destination = dest_file |> File.read!
    modified_contents =
    destination
      |> String.replace(~r/^\s+ENDFIELDS/mi, fields <> "\r\n\r\nENDFIELDS")
      |> String.replace(~r/^\s+ENDGROUPS/mi), field_groups <> "\r\n\r\nENDGROUPS")
      |> String.replace(~r/^\s+INDICES/mi), indexes <> "\r\n\r\nENDINDICES")
      |> String.replace(~r/^\s+ENDMETHODS/mi, methods <> "\r\n\r\nENDMETHODS")
    dest_file
      |> File.write!(modified_contents)
  end

  defp modify_filename(filename) do
    ext = filename |> Path.extname
    (filename |> Path.rootname(ext)) <> "_modified" <> ext
  end
end
