require "./lib/rdb"
require "test/unit"

class RDBTest < Test::Unit::TestCase
  def clean
    rpath = "./data"
    FileUtils.rm_rf(rpath, secure: true)
  end

  def test_create
    clean
    rpath = "./data"
    table_name = "test_tbl"
    RDB::sql("create table #{table_name}(id integer, col1 integer, col2 text);")
    assert_equal(Dir.exist?("#{rpath}/tables/#{table_name}/"), true)
    assert_equal(File.exist?("#{rpath}/tables/#{table_name}/data"), true)
    assert_equal(File.exist?("#{rpath}/tables/#{table_name}/header"), true)
  end

  def test_select
    clean
    assert_equal(
      [
        {
          "id" => 1,
          "col1" => 10,
          "col2" => "test",
        },
      ], RDB::sql("select * from test_tbl")
    )
  end
  def test_insert
    clean
    table_name = "test_tbl"
    RDB::sql("create table #{table_name}(id integer, col1 integer, col2 text);")
    RDB::sql("insert into #{table_name}(id, col1, col2) values(1, 10, 'test');")
    rpath = "./data"
    assert_equal(
      File.read("#{rpath}/tables/#{table_name}/data"),
      "1,10,'test'\n"
    )
  end
  def test_delete
    clean
    table_name = "test_tbl"
    RDB::sql("create table #{table_name}(id integer, col1 integer, col2 text);")
    RDB::sql("delete from #{table_name};")
    rpath = "./data"
    assert_equal(
      File.read("#{rpath}/tables/#{table_name}/data"),
      ""
    )
  end

end
