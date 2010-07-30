module Arel
  module Visitors
    class Sql
      def initialize environment
        @environment = environment
        @engine = environment.engine
      end

      def accept object
        method = :"visit_#{object.class.name.gsub('::', '_')}"
        send method, object
      end

      def visit_Arel_Attribute o
        formatter = Arel::Sql::WhereCondition.new(o.relation)
        formatter.attribute o
      end

      alias :visit_Arel_Sql_Attributes_Integer :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_String :visit_Arel_Attribute
      alias :visit_Arel_Sql_Attributes_Time :visit_Arel_Attribute
    end
  end
end
