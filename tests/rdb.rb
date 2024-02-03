require "./lib/rdb"
require "test/unit"

class RDBTest < Test::Unit::TestCase
  def clean
    rpath = "./data"
    FileUtils.rm_rf(rpath)
  end

  def test_create
    clean
    rpath = "./data"
    table_name = "test_tbl"
    RDB::sql("create table #{table_name}(id integer, col1 integer, col2 text);")
    assert_equal(Dir.exist?("#{rpath}/tables/#{table_name}/"), true)
    assert_equal(File.exist?("#{rpath}/tables/#{table_name}/data"), true)
    assert_equal(File.exist?("#{rpath}/tables/#{table_name}/header"), true)
    clean
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
    RDB::sql()
  end

end
