require 'spec_helper'

module Arel
  describe 'sql visitor' do
    describe "booleans" do
      it "should convert to sql" do
        relation = Table.new :users
        thing = Arel::Sql::Attributes::Boolean.new('column', relation, 'fun')
        thing.to_sql.should == "\"users\".\"fun\""
      end
    end

    describe "descending" do
      it "should convert to sql" do
        relation = Table.new :users
        attribute = relation[:id]
        attribute.desc.to_sql.should == "\"users\".\"id\" DESC"
      end
    end

    describe "average" do
      it "should convert to sql" do
        relation = Table.new :users
        sql = relation.project(SqlLiteral.new("id").average).to_sql
        sql.should =~ /AVG\(id\)/
      end
    end

    describe "min" do
      it "should convert to sql" do
        relation = Table.new :users
        sql = relation.project(SqlLiteral.new("id").minimum).to_sql
        sql.should =~ /MIN\(id\)/
      end
    end

    describe "max" do
      it "should convert to sql" do
        relation = Table.new :users
        sql = relation.project(SqlLiteral.new("id").maximum).to_sql
        sql.should =~ /MAX\(id\)/
      end
    end
  end
end
