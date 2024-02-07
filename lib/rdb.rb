require "pg_query"
require "fileutils"
require "csv"
require 'tempfile'

class TableExistError < StandardError
  attr_reader :attr

  def initialize(msg = "table already exists", attr = "")
    @attr = attr
    super(msg)
  end
end

DATA_PATH = "./data"

class RDB
  def self.a_const_val(aconst)
    if aconst.ival
      aconst.ival.ival
    elsif aconst.sval
      "'#{aconst.sval.sval}'"
    end
  end
  def self.insert(stmts)
    table_name = stmts[0].stmt.insert_stmt.relation.relname
    vals_list = stmts[0].stmt.insert_stmt.select_stmt.select_stmt.values_lists
    table_data_path = "#{DATA_PATH}/tables/#{table_name}/data"
    CSV.open(table_data_path, "a") do |csv|
      vals_list.each do |vals|
        line_ary = []
        vals.list.items.each_with_index do |v,i|
          line_ary.push(self.a_const_val(v.a_const))
        end
        csv << line_ary
      end
    end
  end
  def self.select(stmts)
    table_name = stmts[0].stmt.select_stmt.from_clause[0].range_var.relname
    table_data_path = "#{DATA_PATH}/tables/#{table_name}/data"
    table_header_path = "#{DATA_PATH}/tables/#{table_name}/header"
    headers = File.read(table_header_path).chomp.split(',').map{|x| x.split(':')[0]}
    rows = []
    CSV.foreach(table_data_path, headers: headers) do |row|
      rows << row.to_h
    end
    rows
  end
  def self.create(stmts)
    table_name = stmts[0].stmt.create_stmt.relation.relname
    elts = stmts[0].stmt.create_stmt.table_elts
    table_path = "#{DATA_PATH}/tables/#{table_name}"

    if Dir.exist?(table_path)
      raise TableExistError.new(table_path)
    end

    FileUtils.mkdir_p(table_path)

    header = []
    elts.each do |el|
      col_name = el.column_def.colname
      type_name = el.column_def.type_name.names.filter { |x| x.string.sval != "pg_catalog" }[0].string.sval
      header << "#{col_name}:#{type_name}"
    end
    File.write("#{table_path}/header", header.join(',') + "\n")
    File.write("#{table_path}/data", "")
  end
  def self.delete(stmts)
    table_name = stmts[0].stmt.delete_stmt.relation.relname
    table_data_path = "#{DATA_PATH}/tables/#{table_name}/data"
    File.write(table_data_path, "")
  end
  def self.update(stmts)
    table_name = stmts[0].stmt.update_stmt.relation.relname
    table_data_path = "#{DATA_PATH}/tables/#{table_name}/data"
    table_header_path = "#{DATA_PATH}/tables/#{table_name}/header"
    target_list = stmts[0].stmt.update_stmt.target_list
    update_map = {}
    target_list.each do |target|
      colname = target.res_target.name
      update_map[colname] = self.a_const_val(target.res_target.val.a_const)
    end

    tempfile = Tempfile.new("#{table_data_path}.temp")
    headers = File.read(table_header_path).chomp.split(',').map{|x| x.split(':')[0]}
    CSV.open(tempfile.path, "w") do |csv|
      CSV.foreach(table_data_path, headers: headers) do |row|
        update_map.each do |k,v|
          row[k] = v
        end
        csv << row
      end
    end
    tempfile.close
    FileUtils.mv(tempfile.path, table_data_path)
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
    elsif stmts[0].stmt.update_stmt
      self.update(stmts)
    end
  end
end

def analize(sql)
  stmts = PgQuery.parse(sql).tree.stmts
  puts "select" if stmts[0].stmt.select_stmt
  puts "create" if stmts[0].stmt.create_stmt
end

# puts "# 1"
# sql = "select * from test;"
# stmts = PgQuery.parse(sql).tree.stmts
# pp stmts[0].stmt.select_stmt.from_clause[0].range_var.relname
