require 'spec_helper'

module Arel
  describe 'sql visitor' do
    it "should convert to sql" do
      relation = Table.new :users
      thing = Arel::Sql::Attributes::Boolean.new('column', relation, 'fun')
      thing.to_sql.should == "\"users\".\"fun\""
    end
  end
end
