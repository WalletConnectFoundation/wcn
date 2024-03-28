{
  capitalize(str):: "%s%s" % [
    std.asciiUpper(std.substr(str, 0, 1)),
    std.asciiLower(std.substr(str, 1, std.length(str) - 1))
  ],
}
