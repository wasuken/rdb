require "pg_query"
require "fileutils"
require "csv"

class TableExistError < StandardError
  attr_reader :attr

  def initialize(msg = "table already exists", attr = "")
    @attr = attr
    super(msg)
  end
end

DATA_PATH = "./data"

class RDB
  def self.insert(stmts)
    table_name = stmts[0].stmt.insert_stmt.relation.relname
    vals_list = stmts[0].stmt.insert_stmt.select_stmt.select_stmt.values_lists
    table_data_path = "#{DATA_PATH}/tables/#{table_name}/data"
    CSV.open(table_data_path, "a") do |csv|
      vals_list.each do |vals|
        line_ary = []
        vals.list.items.each_with_index do |v,i|
          vcon = v.a_const
          if vcon.ival
            line_ary.push(vcon.ival.ival)
          elsif vcon.sval
            line_ary.push("'#{vcon.sval.sval}'")
          end
        end
        csv << line_ary
      end
    end
  end
  def self.select(stmts)
    [
      {
        "id" => 1,
        "col1" => 10,
        "col2" => "test",
      },
    ]
  end
  def self.create(stmts)
    table_name = stmts[0].stmt.create_stmt.relation.relname
    elts = stmts[0].stmt.create_stmt.table_elts
    table_path = "#{DATA_PATH}/tables/#{table_name}"

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
  def self.delete(stmts)
    table_name = stmts[0].stmt.delete_stmt.relation.relname
    table_data_path = "#{DATA_PATH}/tables/#{table_name}/data"
    File.write(table_data_path, "")
  end
  def self.sql(sql)
    stmts = PgQuery.parse(sql).tree.stmts
    if stmts[0].stmt.select_stmt
      self.select(stmts)
    elsif stmts[0].stmt.create_stmt
      self.create(stmts)
    elsif stmts[0].stmt.insert_stmt
      self.insert(stmts)
    elsif stmts[0].stmt.delete_stmt
      self.delete(stmts)
    end
  end
end

def analize(sql)
  stmts = PgQuery.parse(sql).tree.stmts
  puts "select" if stmts[0].stmt.select_stmt
  puts "create" if stmts[0].stmt.create_stmt
end

# puts "# 1"
# sql = "insert into test_tbl(id, col1, col2) values(1, 10, 'test');"
# stmts = PgQuery.parse(sql).tree.stmts
# pp stmts[0].stmt.insert_stmt.select_stmt.select_stmt.values_lists
