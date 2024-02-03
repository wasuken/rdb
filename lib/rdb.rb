require "pg_query"
require "fileutils"

class TableExistError < StandardError
  attr_reader :attr

  def initialize(msg = "table already exists", attr = "")
    @attr = attr
    super(msg)
  end
end

class RDB
  def self.sql(sql)
    rpath = "./data"
    stmts = PgQuery.parse(sql).tree.stmts
    if stmts[0].stmt.select_stmt
      [
        {
          "id" => 1,
          "col1" => 10,
          "col2" => "test",
        },
      ]
    elsif stmts[0].stmt.create_stmt
      table_name = stmts[0].stmt.create_stmt.relation.relname
      elts = stmts[0].stmt.create_stmt.table_elts
      table_path = "#{rpath}/tables/#{table_name}"

      if Dir.exist?(table_path)
        raise TableExistError.new(table_path)
      end

      FileUtils.mkdir_p(table_path)

      elts.each do |el|
        col_name = el.column_def.colname
        type_name = el.column_def.type_name.names.filter { |x| x.string.sval != "pg_catalog" }[0].string.sval
        line = "#{col_name}:#{type_name}\n"
        File.write("#{table_path}/header", line)
      end
      File.write("#{table_path}/data", "")
    end
  end
end

def analize(sql)
  stmts = PgQuery.parse(sql).tree.stmts
  puts "select" if stmts[0].stmt.select_stmt
  puts "create" if stmts[0].stmt.create_stmt
end

# puts "# 1"
# sql = "create table test_tbl(id integer, col1 integer, col2 text);"
# stmts = PgQuery.parse(sql).tree.stmts
# pp stmts
