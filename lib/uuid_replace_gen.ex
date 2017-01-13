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

  def transform_replace(source_filename, dest_filename, output_folder, prefix \\ "mms", replacement_map \\ %{"WAX" => "WHS", "TRX" => "TMS"}) do
    source_filename
      |> transform(dest_filename, output_folder, prefix)
      |> replace_prefixes_in_files_1(replacement_map)
  end

  def transform(source_file, dest_file, output_folder, prefix \\ "mms") do
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
    destination = dest_file |> File.read!
    {added_modified_methods, new_destination, was_destination_modified} =
      process_modified_methods(source, destination, prefix)
    methods =
      ((~S|\s*SOURCE\s+\#| <> prefix <> ~S|\w+[\w\W]*ENDSOURCE|)
      |> extract_transform(source)) <> added_modified_methods
      # |> Regex.compile!([:caseless, :multiline, :ungreedy])
      # |> Regex.scan(source)
      # |> Stream.map(&(hd(&1) <> "\r\n\r\n"))
      # |> Enum.join
    events =
      (~S|\s*METHOD\s+\#| <> prefix <> ~S|\w+[\w\W]*ENDMETHOD|)
      |> extract_transform(source)
    modified_contents =
    (if was_destination_modified, do: new_destination, else: destination)
      |> String.replace(~r/^\s+ENDFIELDS/mi, fields <> ~S|\0|)
      |> String.replace(~r/^\s+ENDGROUPS/mi, field_groups <> ~S|\0|)
      |> String.replace(~r/^\s+ENDINDICES/mi, indexes <> ~S|\0|)
      |> String.replace(~r/^\s+ENDREFERENCES/mi, references <> ~S|\0|)
      |> String.replace(~r/^\s*ENDDELETEACTIONS/mi, delete_actions <> ~S|\0|)
      |> String.replace(~r/^\s+ENDMETHODS/mi, methods <> ~S|\0|)
      |> String.replace(~r/^\s*ENDEVENTS/mi, events <> ~S|\0|)
    new_file_name =
      dest_file
      |> transform_destination_filename(was_destination_modified, output_folder)
    new_file_name
      |> File.write!(modified_contents)
    new_file_name
  end

  defp transform_destination_filename(filename, was_modified, output_folder) do
    if was_modified, do: filename |> add_prefix("modified___", output_folder), else: filename |> add_prefix("clear___", output_folder)
  end

  defp add_prefix(filename, prefix, output_folder) do
    Path.join([output_folder, prefix <> Path.basename(filename)])
  end

  def prefix_to_not_prefix_group(prefix) do
    prefix |> String.codepoints |> Stream.map(&("[^#{&1}]")) |> Enum.join
  end

  defp is_prefixed?(name, prefix) do
    (~S"^\s*" <> prefix) |> Regex.compile!([:caseless]) |> Regex.match?(name)
  end

  def transform_files(source_folder, destination_folder, output_folder, prefix \\ "mms", replacement_map \\ %{"WAX" => "WHS", "TRX" => "TMS"}) do
    source_folder
      |> File.ls!
      |> Enum.each(
        &(if &1 |> has_counterpart?(destination_folder) do
          transform_replace(source_folder |> Path.join(&1), &1 |> counterpart_filename(destination_folder), output_folder, prefix, replacement_map)
          IO.puts "Tramsformed #{&1}"
        else
          File.copy!(source_folder |> Path.join(&1), output_folder |> Path.join((if &1 |> is_prefixed?(prefix), do: "New___", else: "Unmapped___") <> &1))
          IO.puts "Copied as new #{&1}"
        end))
  end

  defp process_modified_methods(source, destination, prefix \\ "mms") do
    {new_methods, upd_destination, destination_updated} =
      (~S|\s*SOURCE\s+\#((?!| <> prefix <> ~S|)[\w_]+\b)[\w\W]*ENDSOURCE|)
      |> Regex.compile!([:caseless, :ungreedy])
      |> Regex.scan(source)
      |> Enum.reduce({"", destination, false}, fn [x, y], {new_methods, destination_modified, is_modified} ->
        if ((~S"[\s\w_]?" <> prefix) |> Regex.compile!([:caseless]) |> Regex.match?(x)) do
          if has_method_defined?(destination_modified, y) do
            {new_methods, replace_method_definition(destination_modified, y, x), true}
          else
            {new_methods <> "\r\n" <> x, destination_modified, is_modified}
          end
        else
          {new_methods, destination_modified, is_modified}
        end
      end)
  end

  defp build_start_method_regex(method_name) do
    ~S"\s*SOURCE\s+\#" <> method_name <> ~S"\b"
  end

  defp has_method_defined?(source, method_name) do
    method_name |> build_start_method_regex |> Regex.compile!([:caseless]) |> Regex.match?(source)
  end

  defp replace_method_definition(source, method_name, replacement) do
    ((method_name |> build_start_method_regex) <> ~S"[\w\W]*ENDSOURCE")
      |> Regex.compile!([:caseless, :ungreedy])
      |> Regex.replace(source, replacement)
  end

  def counterpart_name(filename, prefixes_map \\ %{"WAX" => "WHS", "TRX" => "TMS"}) do
    case "(" <> (prefixes_map |> Map.keys |> Enum.join("|")) <> ")" |> Regex.compile!([:caseless]) |> Regex.run(filename) do
      [h|_t] -> filename |> String.replace_prefix(h, prefixes_map[h])
      nil -> filename
    end
  end

  def transform_object_names(names, replacement_map \\ %{"WAX" => "WHS", "TRX" => "TMS"}) do
    (~S"(?<=[\s\w_])(" <> (replacement_map |> Map.keys |> Enum.join("|")) <> ")")
      |> Regex.compile!([:caseless, :multiline, :ungreedy])
      |> Regex.replace(names, fn _, x -> replacement_map[x |> String.upcase] end, global: true)
  end

  def counterpart_names(name, replacement_map \\ %{"WAX" => ["WMS", "WHS"], "TRX" => ["TMS"]}) do
    regex =
    ~S"^(" <> (replacement_map |> Map.keys |> Enum.join("|")) <> ")"
      |> Regex.compile!([:caseless])
    case regex |> Regex.run(name) do
      [_,x] ->
        replacement_map[x] |> Stream.map(&(regex |> Regex.replace(name, &1)))
      _ -> nil
    end
  end

  def get_counterpart_name(name, replacement_map \\ %{"WAX" => "WHS", "TRX" => "TMS"}) do
    ~S"^(" <> (replacement_map |> Map.keys |> Enum.join("|")) <> ")"
      |> Regex.compile!([:caseless])
      |> Regex.replace(name, fn _, x -> replacement_map[x |> String.upcase] end)
  end

  def counterpart_filename(filename, counterpart_folder) do
    counterpart_folder |> Path.join(filename |> get_counterpart_name)
  end

  def has_counterpart?(filename, counterpart_folder) do
    counterpart_filename(filename, counterpart_folder) |> File.exists?
  end

  def transform_names_file(filename) do
    transformed = filename |> File.read! |> transform_object_names
    filename |> modify_filename |> File.write!(transformed)
  end

  def replace_prefixes(source, replacement_map \\ %{"WAX" => "WHS", "TRX" => "TMS"}) do
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
  def replace_prefixes_in_files(filename, replacement_map \\ %{"WAX" => "WHS", "TRX" => "TMS"}) do
    write_handle = filename |> modify_filename|> File.open!([:write, encoding: :latin1])
    filename |> File.stream!([encoding: :latin1], :line) |> Stream.map(fn frame -> frame |> replace_prefixes(replacement_map) end)
      |> Enum.each(&(write_handle |> IO.puts(&1 <> "\r\n")))
    write_handle |> File.close
  end

  def replace_prefixes_in_files_1(filename, replacement_map \\ %{"WAX" => "WHS", "TRX" => "TMS"}) do
    contents = filename |> File.read! |> replace_prefixes(replacement_map)
    filename |> File.write!(contents)
  end

  defp modify_filename(filename) do
    ext = filename |> Path.extname
    (filename |> Path.rootname(ext)) <> "_modified" <> ext
  end
end
