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
      |> Stream.map(&((hd(&1) <> "\r\n\r\n") |> transformation.()))
      |> Enum.join
  end

  def transform_replace(source_filename, dest_filename, prefix \\ "mms", replacement_map \\ %{"WAX" => "WMS"}) do
    transform source_filename, dest_filename, prefix
    replace_prefixes_in_files_1 dest_filename, replacement_map
  end

  def transform(source_file, dest_file, prefix \\ "mms") do
    source = source_file |> File.read!
    fields =
      (~S|\s*FIELD\s+\#| <> prefix <> ~S|\w+[\w\W]*ENDPROPERTIES|)
      |> extract_transform(source, &modify/1)
      # |> Regex.compile!([:caseless, :multiline, :ungreedy])
      # |> Regex.scan(source)
      # |> Stream.map(&((hd(&1) <> "\r\n\r\n") |> modify))
      # |> Enum.join
    field_groups =
      (~S|\s*GROUP\s+\#| <> prefix <> ~S|\w+[\s\W]*ENDGROUP|)
      |> extract_transform(source)
    indexes =
      (~S|^\s*\#| <> prefix <> ~S|\w+\s*$[\w\W]*ENDINDEXFIELDS|)
      |> extract_transform(source)
    references =
      (~S|^\s*REFERENCE\s+\#| <> prefix <> ~S|\w+[\w\W]*ENDREFERENCE|)
      |> extract_transform(source)
    delete_actions =
      (~S|^\s*\#| <> prefix <> ~S|\w+\(\w*\)[\w\W]*ENDPROPERTIES|)
      |> extract_transform(source)
    methods =
      (~S|\s*SOURCE\s+\#| <> prefix <> ~S|\w+[\w\W]*ENDSOURCE|)
      |> extract_transform(source)
      # |> Regex.compile!([:caseless, :multiline, :ungreedy])
      # |> Regex.scan(source)
      # |> Stream.map(&(hd(&1) <> "\r\n\r\n"))
      # |> Enum.join
    events =
      (~S|\s*METHOD\s+\#| <> prefix <> ~S|\w+[\w\W]*ENDMETHOD|)
      |> extract_transform(source)
    destination = dest_file |> File.read!
    modified_contents =
    destination
      |> String.replace(~r/^\s+ENDFIELDS/mi, fields <> ~S|\0|)
      |> String.replace(~r/^\s+ENDGROUPS/mi, field_groups <> ~S|\0|)
      |> String.replace(~r/^\s+ENDINDICES/mi, indexes <> ~S|\0|)
      |> String.replace(~r/^\s+ENDREFERENCES/mi, references <> ~S|\0|)
      |> String.replace(~r/^\s*ENDDELETEACTIONS/mi, delete_actions <> ~S|\0|)
      |> String.replace(~r/^\s+ENDMETHODS/mi, methods <> ~S|\0|)
      |> String.replace(~r/^\s*ENDEVENTS/mi, events <> ~S|\0|)
    dest_file
      |> File.write!(modified_contents)
  end

  def counterpart_name(filename, prefixes_map \\ %{"WAX" => "WMS", "TRX" => "TMS", "MMS_WAX" => "MMS_WMS", "MMS_TRX" => "MMS_TMS"}) do
    case "(" <> (prefixes_map |> Map.keys |> Enum.join("|")) <> ")" |> Regex.compile!([:caseless]) |> Regex.run(filename) do
      [h|_t] -> filename |> String.replace_prefix(h, prefixes_map[h])
      nil -> filename
    end
  end

  def transform_object_names(names, replacement_map \\ %{"WAX" => "WMS", "TRX" => "TMS", "MMS_WAX" => "MMS_WMS", "MMS_TRX" => "MMS_TMS"}) do
    (~S"(?<=[\s\w_])(" <> (replacement_map |> Map.keys |> Enum.join("|")) <> ")")
      |> Regex.compile!([:caseless, :multiline, :ungreedy])
      |> Regex.replace(names, fn _, x -> replacement_map[x |> String.upcase] end, global: true)
  end

  def transform_names_file(filename) do
    transformed = filename |> File.read! |> transform_object_names
    filename |> modify_filename |> File.write!(transformed)
  end

  def replace_prefixes(source, replacement_map \\ %{"WAX" => "WMS", "TRX" => "TMS", "MMS_WAX" => "MMS_WMS", "MMS_TRX" => "MMS_TMS"}) do
    build_stage1_pattern = &(~S"(?<=[\w\s\#\(\!\_\\])(" <> (replacement_map |> Map.keys |> Stream.map(&1) |> Enum.join("|")) <> ")")
    general_replacement_function = &(fn x -> &1[&2.(x)] end) # Performs a lookup in a map given as a first argument a key returned by the function, given as the second
    replacement_function1 = general_replacement_function.((for {k, v} <- replacement_map, into: %{}, do: {k |> String.upcase, v |> String.upcase}), &String.upcase/1)
    regex_pattern1 = build_stage1_pattern.(&String.upcase/1)
    # ~S"(?<=[\s\#\(\!\\])(" <> (replacement_map |> Map.keys |> Stream.map(&(&1 |> String.upcase)) |> Enum.join("|")) <> ")"
    IO.puts "Regex search pattern1: #{regex_pattern1}"
    stage1 =
    regex_pattern1
      |> Regex.compile!([:multiline])
      |> Regex.replace(source, fn y, x -> if y |> String.starts_with?("."), do: ".", else: replacement_function1.(x) end, global: true)
      # |> Regex.replace(source, fn y, x -> if y |> String.starts_with?("."), do: ".", else: replacement_map[x |> String.upcase] |> String.upcase || x end, global: true)
    replacement_function2 = general_replacement_function.((for {k, v} <- replacement_map, into: %{}, do: {k |> String.downcase, v |> String.downcase}), &String.downcase/1)
    regex_pattern2 = build_stage1_pattern.(&String.downcase/1)
    IO.puts "Regex search pattern2: #{regex_pattern2}"
    stage2 =
    regex_pattern2
      |> Regex.compile!([:multiline])
      |> Regex.replace(stage1, replacement_function2, global: true)
    ~S"(?<=\.)(" <> (replacement_map |> Map.keys |> Enum.join("|")) <> ")"
      |> Regex.compile!([:caseless, :multiline])
      |> Regex.replace(stage2, "")
  end

  # Use replace_prefixes_in_files_1 function instead
  def replace_prefixes_in_files(filename, replacement_map \\ %{"WAX" => "WMS", "TRX" => "TMS", "MMS_WAX" => "MMS_WMS", "MMS_TRX" => "MMS_TMS"}) do
    write_handle = filename |> modify_filename|> File.open!([:write, encoding: :latin1])
    filename |> File.stream!([encoding: :latin1], :line) |> Stream.map(fn frame -> frame |> replace_prefixes(replacement_map) end)
      |> Enum.each(&(write_handle |> IO.puts(&1 <> "\r\n")))
    write_handle |> File.close
  end

  def replace_prefixes_in_files_1(filename, replacement_map \\ %{"WAX" => "WMS", "TRX" => "TMS", "MMS_WAX" => "MMS_WMS", "MMS_TRX" => "MMS_TMS"}) do
    contents = filename |> File.read! |> replace_prefixes(replacement_map)
    filename |> modify_filename |> File.write!(contents)
  end

  defp modify_filename(filename) do
    ext = filename |> Path.extname
    (filename |> Path.rootname(ext)) <> "_modified" <> ext
  end
end
