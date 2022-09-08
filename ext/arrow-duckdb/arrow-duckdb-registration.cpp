/*
 * Copyright 2021  Sutou Kouhei <kou@clear-code.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <arrow-glib/arrow-glib.hpp>

#include <arrow/c/bridge.h>
#include <arrow/dataset/api.h>

#include <rbgobject.h>

#include <duckdb.hpp>
#ifndef DUCKDB_AMALGAMATION
#  include <duckdb.h>
#  include <duckdb/common/arrow/arrow_wrapper.hpp>
#  include <duckdb/function/table/arrow.hpp>
#  include <duckdb/function/table_function.hpp>
#  include <duckdb/main/connection.hpp>
#  include <duckdb/planner/filter/conjunction_filter.hpp>
#  include <duckdb/planner/filter/constant_filter.hpp>
#  include <duckdb/planner/table_filter.hpp>
#endif

#include "arrow-duckdb-registration.hpp"

namespace {
  std::shared_ptr<arrow::Scalar>
  convert_constant_timestamp(duckdb::Value &value, arrow::TimeUnit::type unit)
  {
    auto scalar_result =
      arrow::MakeScalar(arrow::timestamp(unit), value.GetValue<int64_t>());
    if (!scalar_result.ok()) {
      throw duckdb::InvalidInputException(
        "[arrow][filter][pushdown][%s] "
        "failed to convert to Apache Arrow scalar: %s: <%s>",
        value.type().ToString(),
        scalar_result.status().ToString(),
        value.ToString());
    }
    return *scalar_result;
  }

  std::shared_ptr<arrow::Scalar>
  convert_constant(duckdb::Value &value)
  {
    switch (value.type().id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return arrow::MakeScalar(value.GetValue<bool>());
    case duckdb::LogicalTypeId::TINYINT:
      return arrow::MakeScalar(value.GetValue<int8_t>());
    case duckdb::LogicalTypeId::SMALLINT:
      return arrow::MakeScalar(value.GetValue<int16_t>());
    case duckdb::LogicalTypeId::INTEGER:
      return arrow::MakeScalar(value.GetValue<int32_t>());
    case duckdb::LogicalTypeId::BIGINT:
      return arrow::MakeScalar(value.GetValue<int64_t>());
    // case duckdb::LogicalTypeId::HUGEINT:
    //   return arrow::MakeScalar(value.GetValue<duckdb::hugeint_t>());
    // case duckdb::LogicalTypeId::DATE:
    //   return arrow::MakeScalar(arrow::date32(), value.GetValue<int32_t>());
    // case duckdb::LogicalTypeId::TIME:
    //   return arrow::MakeScalar(arrow::time64(), value.GetValue<int64_t>());
    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
      return convert_constant_timestamp(value, arrow::TimeUnit::SECOND);
    case duckdb::LogicalTypeId::TIMESTAMP_MS:
      return convert_constant_timestamp(value, arrow::TimeUnit::MILLI);
    case duckdb::LogicalTypeId::TIMESTAMP:
      return convert_constant_timestamp(value, arrow::TimeUnit::MICRO);
    case duckdb::LogicalTypeId::TIMESTAMP_NS:
      return convert_constant_timestamp(value, arrow::TimeUnit::NANO);
    case duckdb::LogicalTypeId::UTINYINT:
      return arrow::MakeScalar(value.GetValue<uint8_t>());
    case duckdb::LogicalTypeId::USMALLINT:
      return arrow::MakeScalar(value.GetValue<uint16_t>());
    case duckdb::LogicalTypeId::UINTEGER:
      return arrow::MakeScalar(value.GetValue<uint32_t>());
    case duckdb::LogicalTypeId::UBIGINT:
      return arrow::MakeScalar(value.GetValue<uint64_t>());
    case duckdb::LogicalTypeId::FLOAT:
      return arrow::MakeScalar(value.GetValue<float>());
    case duckdb::LogicalTypeId::DOUBLE:
      return arrow::MakeScalar(value.GetValue<double>());
    case duckdb::LogicalTypeId::VARCHAR:
      return arrow::MakeScalar(value.ToString());
    // case LogicalTypeId::DECIMAL:
    default:
      throw duckdb::NotImplementedException(
        "[arrow][filter][pushdown][%s] not implemented value type",
        value.type().ToString());
    }
  }

  arrow::compute::Expression
  convert_filter(duckdb::TableFilter *filter,
                 std::string &column_name)
  {
    auto field = arrow::compute::field_ref(column_name);
    switch (filter->filter_type) {
    case duckdb::TableFilterType::CONSTANT_COMPARISON:
      {
        auto constant_filter = static_cast<duckdb::ConstantFilter *>(filter);
        auto constant_scalar = convert_constant(constant_filter->constant);
        auto constant = arrow::compute::literal(constant_scalar);
        switch (constant_filter->comparison_type) {
        case duckdb::ExpressionType::COMPARE_EQUAL:
          return arrow::compute::equal(field, constant);
        case duckdb::ExpressionType::COMPARE_LESSTHAN:
          return arrow::compute::less(field, constant);
        case duckdb::ExpressionType::COMPARE_GREATERTHAN:
          return arrow::compute::greater(field, constant);
        case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
          return arrow::compute::less_equal(field, constant);
        case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
          return arrow::compute::greater_equal(field, constant);
        default:
          throw duckdb::NotImplementedException(
            "[arrow][filter][pushdown] not implemented comparison type: %s",
            duckdb::ExpressionTypeToString(constant_filter->comparison_type));
        }
        break;
      }
    case duckdb::TableFilterType::IS_NULL:
      return arrow::compute::is_null(field);
    case duckdb::TableFilterType::IS_NOT_NULL:
      return arrow::compute::is_valid(field);
    case duckdb::TableFilterType::CONJUNCTION_OR:
      {
        auto or_filter = static_cast<duckdb::ConjunctionOrFilter *>(filter);
        std::vector<arrow::compute::Expression> sub_expressions;
        for (auto &child_filter : or_filter->child_filters) {
          sub_expressions.emplace_back(
            std::move(convert_filter(child_filter.get(), column_name)));
        }
        return arrow::compute::or_(sub_expressions);
      }
    case duckdb::TableFilterType::CONJUNCTION_AND:
      {
        auto and_filter = static_cast<duckdb::ConjunctionAndFilter *>(filter);
        std::vector<arrow::compute::Expression> sub_expressions;
        for (auto &child_filter : and_filter->child_filters) {
          sub_expressions.emplace_back(
            std::move(convert_filter(child_filter.get(), column_name)));
        }
        return arrow::compute::and_(sub_expressions);
      }
    default:
      throw duckdb::NotImplementedException(
        "[arrow][filter][pushdown] unknown filter type: %u",
        filter->filter_type);
    }
  }

  arrow::compute::Expression
  convert_filters(std::unordered_map<
                    idx_t,
                    std::unique_ptr<duckdb::TableFilter>
                  > &filters,
                  std::unordered_map<idx_t, std::string> &column_names)
  {
    std::vector<arrow::compute::Expression> expressions;
    for (auto it = filters.begin(); it != filters.end(); ++it) {
      expressions.emplace_back(
        std::move(convert_filter(it->second.get(), column_names[it->first])));
    }
    return arrow::compute::and_(expressions);
  }

  arrow::Result<std::unique_ptr<duckdb::ArrowArrayStreamWrapper>>
  arrow_table_produce_internal(uintptr_t data,
                               duckdb::ArrowStreamParameters &parameters)
  {
    auto garrow_table = GARROW_TABLE(reinterpret_cast<gpointer>(data));
    auto arrow_table = garrow_table_get_raw(garrow_table);
    auto dataset =
      std::make_shared<arrow::dataset::InMemoryDataset>(arrow_table);
    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    bool have_filter =
      parameters.filters &&
      !parameters.filters->filters.empty();
    if (have_filter) {
      ARROW_RETURN_NOT_OK(
        scanner_builder->Filter(
          convert_filters(parameters.filters->filters,
                          parameters.projected_columns.projection_map)));
    }
    if (!parameters.projected_columns.columns.empty()) {
      ARROW_RETURN_NOT_OK(
        scanner_builder->Project(
          parameters.projected_columns.columns));
    }
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    ARROW_ASSIGN_OR_RAISE(auto reader, scanner->ToRecordBatchReader());
    auto stream_wrapper = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
    ARROW_RETURN_NOT_OK(
      arrow::ExportRecordBatchReader(reader,
                                     &(stream_wrapper->arrow_array_stream)));
    return stream_wrapper;
  }

  std::unique_ptr<duckdb::ArrowArrayStreamWrapper>
  arrow_table_produce(uintptr_t data,
                      duckdb::ArrowStreamParameters &parameters)
  {
    auto stream_wrapper_result = arrow_table_produce_internal(data, parameters);
    if (!stream_wrapper_result.ok()) {
      throw std::runtime_error(
        std::string("[arrow][produce] failed to produce: ") +
        stream_wrapper_result.status().ToString());
    }
    return std::move(*stream_wrapper_result);
  }

  void
  arrow_table_get_schema(uintptr_t data, duckdb::ArrowSchemaWrapper &schema)
  {
    auto garrow_table = GARROW_TABLE(reinterpret_cast<gpointer>(data));
    auto arrow_table = garrow_table_get_raw(garrow_table);
    arrow::ExportSchema(*(arrow_table->schema()),
                        reinterpret_cast<ArrowSchema *>(&schema));
  }
}

namespace arrow_duckdb {
  void
  connection_unregister(duckdb_connection connection, VALUE name)
  {
    auto c_name = StringValueCStr(name);
    reinterpret_cast<duckdb::Connection *>(connection)
      ->Query(std::string("DROP VIEW \"") + c_name + "\"");
  }

  void
  connection_register(duckdb_connection connection,
                      VALUE name,
                      VALUE arrow_table)
  {
    auto c_name = StringValueCStr(name);
    auto garrow_table = RVAL2GOBJ(arrow_table);
    reinterpret_cast<duckdb::Connection *>(connection)
      ->TableFunction(
        "arrow_scan",
        {
          duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(garrow_table)),
          duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(arrow_table_produce)),
          duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(arrow_table_get_schema)),
        })
      ->CreateView(c_name, true, true);
  }
}
