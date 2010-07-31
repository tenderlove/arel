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
  end
end
