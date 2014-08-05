xdiff() {
  rm -f /tmp/b
  if [ -f "$1" ]; then
  awk '
/^package / {packageSeen = 1}
!packageSeen {next}
{
  if (0) {
  gsub(/BeeLine/, "FooLine");
  gsub(/beeLine/, "fooLine");
  gsub(/beeline/, "fooline");
  } else {
  gsub(/BeeLine/, "SqlLine");
  gsub(/beeLine/, "sqlLine");
  gsub(/beeline/, "sqlline");
  }
  gsub(/SimpleCompletor/, "StringsCompleter");
  gsub(/ompletor/, "ompleter");
  gsub(/XMLAttr/, "XmlAttr");
  gsub(/XMLEl/, "XmlEl");
  print;
}
      ' "$1" > /tmp/b
  fi

  rm -f /tmp/s
  if [ -f "$2" ]; then
  awk '
/^package / {packageSeen = 1}
!packageSeen {next}
{
  if (0) {
  gsub(/SqlLine/, "FooLine");
  gsub(/sqlLine/, "fooLine");
  gsub(/sqlline/, "fooline");
  }
  print;
}
      ' "$2" > /tmp/s
  fi

  cp /tmp/b /tmp/$(basename "$2")
  diff -u /tmp/b /tmp/s
}


for b in $(find ../hive.2/beeline -name \*.java); do
  s=$(echo $b | sed -e 's/beeline/sqlline/g;s/hive.2/hive/;s/Bee/Sql/;s/ompletor/ompleter/;s/XMLAttr/XmlAttr/;s/XMLEl/XmlEl/;')
  echo
  echo :: diff $b $s ::
  xdiff $b $s
done
