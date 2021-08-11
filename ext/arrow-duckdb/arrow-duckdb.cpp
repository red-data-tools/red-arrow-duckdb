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

#include <ruby-duckdb.h>

#include <duckdb.h>
#include <duckdb.hpp>
#ifndef DUCKDB_AMALGAMATION
#  include <duckdb/common/arrow_wrapper.hpp>
#  include <duckdb/function/table_function.hpp>
#  include <duckdb/main/connection.hpp>
#  include <duckdb/planner/filter/conjunction_filter.hpp>
#  include <duckdb/planner/filter/constant_filter.hpp>
#  include <duckdb/planner/table_filter.hpp>
#endif

extern "C" void Init_arrow_duckdb(void);

namespace {
#  if !GARROW_VERSION_CHECK(6, 0, 0)
  GArrowSchema *
  garrow_schema_import(gpointer c_abi_schema,
                       GError **error)
  {
    auto arrow_schema =
      *arrow::ImportSchema(static_cast<ArrowSchema *>(c_abi_schema));
    return garrow_schema_new_raw(&arrow_schema);
  }

  GArrowRecordBatch *
  garrow_record_batch_import(gpointer c_abi_array,
                             GArrowSchema *schema,
                             GError **error)
  {
    auto arrow_schema = garrow_schema_get_raw(schema);
    auto arrow_record_batch =
      *arrow::ImportRecordBatch(static_cast<ArrowArray *>(c_abi_array),
                                arrow_schema);
    return garrow_record_batch_new_raw(&arrow_record_batch);
  }
#  endif

  VALUE cArrowTable;
  VALUE cArrowDuckDBResult;

  struct Result {
    duckdb_arrow arrow;
    char *error_message;
    GArrowSchema *gschema;
  };

  void
  result_free(void *data)
  {
    Result *result = static_cast<Result *>(data);
    if (result->gschema) {
      g_object_unref(result->gschema);
    }
    free(result->error_message);
    duckdb_destroy_arrow(&(result->arrow));
  }

  static const rb_data_type_t result_type = {
    "ArrowDuckDB::Result",
    {
      nullptr,
      result_free,
    },
    nullptr,
    nullptr,
    RUBY_TYPED_FREE_IMMEDIATELY,
  };

  VALUE
  result_alloc_func(VALUE klass)
  {
    Result *result;
    auto rb_result = TypedData_Make_Struct(klass,
                                           Result,
                                           &result_type,
                                           result);
    result->arrow = nullptr;
    result->error_message = nullptr;
    result->gschema = nullptr;
    return rb_result;
  }

  void
  result_ensure_gschema(Result *result)
  {
    ArrowSchema c_abi_schema;
    duckdb_arrow_schema schema = &c_abi_schema;
    auto state = duckdb_query_arrow_schema(result->arrow, &schema);
    if (state == DuckDBError) {
      free(result->error_message);
      result->error_message =
        const_cast<char *>(duckdb_query_arrow_error(result->arrow));
      rb_raise(eDuckDBError,
               "Failed to fetch Apache Arrow schema: %s",
               result->error_message);
    }

    GError *gerror = nullptr;
    result->gschema = garrow_schema_import(&c_abi_schema, &gerror);
    if (gerror) {
      RG_RAISE_ERROR(gerror);
    }
  }

  VALUE
  result_fetch(VALUE self)
  {
    Result *result;
    TypedData_Get_Struct(self, Result, &result_type, result);

    result_ensure_gschema(result);

    ArrowArray c_abi_array = {};
    duckdb_arrow_array array = &c_abi_array;
    auto state = duckdb_query_arrow_array(result->arrow, &array);
    if (state == DuckDBError) {
      free(result->error_message);
      result->error_message =
        const_cast<char *>(duckdb_query_arrow_error(result->arrow));
      rb_raise(eDuckDBError,
               "Failed to fetch Apache Arrow array: %s",
               result->error_message);
    }
    if (!c_abi_array.release) {
      return Qnil;
    }

    GError *gerror = nullptr;
    auto grecord_batch = garrow_record_batch_import(&c_abi_array,
                                                    result->gschema,
                                                    &gerror);
    if (gerror) {
      RG_RAISE_ERROR(gerror);
      return Qnil;
    }
    return GOBJ2RVAL_UNREF(grecord_batch);
  }

  VALUE
  query_sql_arrow(VALUE self, VALUE sql)
  {
    rubyDuckDBConnection *ctx;
    Data_Get_Struct(self, rubyDuckDBConnection, ctx);

    if (!(ctx->con)) {
      rb_raise(eDuckDBError, "Database connection closed");
    }

    ID id_new;
    CONST_ID(id_new, "new");
    auto result = rb_funcall(cArrowDuckDBResult, id_new, 0);
    Result *arrow_duckdb_result;
    TypedData_Get_Struct(result, Result, &result_type, arrow_duckdb_result);
    auto state = duckdb_query_arrow(ctx->con,
                                    StringValueCStr(sql),
                                    &(arrow_duckdb_result->arrow));
    if (state == DuckDBError) {
      if (arrow_duckdb_result->arrow) {
        arrow_duckdb_result->error_message =
          const_cast<char *>(
            duckdb_query_arrow_error(arrow_duckdb_result->arrow));
        rb_raise(eDuckDBError,
                 "Failed to execute query: %s",
                 arrow_duckdb_result->error_message);
      } else {
        rb_raise(eDuckDBError, "Failed to execute query");
      }
    }

    return result;
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
    // case duckdb::LogicalTypeId::TIMESTAMP:
    //   return arrow::MakeScalar(arrow::timestamp(),
    //                            value.GetValue<int64_t>());
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
        "[arrow][filter][pushdown] not implemented value type: %s",
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
                               std::pair<
                                 std::unordered_map<idx_t, std::string>,
                                 std::vector<std::string>
                               > &project_columns,
                               duckdb::TableFilterCollection *filters)
  {
    auto garrow_table = GARROW_TABLE(reinterpret_cast<gpointer>(data));
    auto arrow_table = garrow_table_get_raw(garrow_table);
    auto dataset =
      std::make_shared<arrow::dataset::InMemoryDataset>(arrow_table);
    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    bool have_filter =
      filters &&
      filters->table_filters &&
      !filters->table_filters->filters.empty();
    if (have_filter) {
      ARROW_RETURN_NOT_OK(
        scanner_builder->Filter(convert_filters(filters->table_filters->filters,
                                                project_columns.first)));
    }
    if (!project_columns.second.empty()) {
      ARROW_RETURN_NOT_OK(scanner_builder->Project(project_columns.second));
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
                      std::pair<
                        std::unordered_map<idx_t, std::string>,
                        std::vector<std::string>
                      > &project_columns,
                      duckdb::TableFilterCollection *filters)
  {
    auto stream_wrapper_result =
      arrow_table_produce_internal(data, project_columns, filters);
    if (!stream_wrapper_result.ok()) {
      throw std::runtime_error(
        std::string("[arrow][produce] failed to produce: ") +
        stream_wrapper_result.status().ToString());
    }
    return std::move(*stream_wrapper_result);
  }

  VALUE
  query_unregister_arrow(VALUE self, VALUE name)
  {
    rubyDuckDBConnection *ctx;
    Data_Get_Struct(self, rubyDuckDBConnection, ctx);

    if (!(ctx->con)) {
      rb_raise(eDuckDBError, "Database connection closed");
    }

    auto c_name = StringValueCStr(name);
    reinterpret_cast<duckdb::Connection *>(ctx->con)
      ->Query(std::string("DROP VIEW \"") + c_name + "\"");

    auto arrow_tables = rb_iv_get(self, "@arrow_tables");
    if (NIL_P(arrow_tables)) {
      arrow_tables = rb_hash_new();
      rb_iv_set(self, "@arrow_tables", arrow_tables);
    }
    rb_hash_delete(arrow_tables, name);

    return self;
  }

  VALUE
  query_register_arrow_body(VALUE)
  {
    return rb_yield_values(0);
  }

  struct QueryRegisterArrowData {
    VALUE self;
    VALUE name;
  };

  VALUE
  query_register_arrow_ensure(VALUE user_data)
  {
    auto data = reinterpret_cast<QueryRegisterArrowData *>(user_data);
    return query_unregister_arrow(data->self, data->name);
  }

  VALUE
  query_register_arrow(VALUE self, VALUE name, VALUE arrow_table)
  {
    rubyDuckDBConnection *ctx;
    Data_Get_Struct(self, rubyDuckDBConnection, ctx);

    if (!(ctx->con)) {
      rb_raise(eDuckDBError, "Database connection closed");
    }

    auto c_name = StringValueCStr(name);
    if (!RVAL2CBOOL(rb_obj_is_kind_of(arrow_table, cArrowTable))) {
      rb_raise(rb_eArgError, "must be Arrow::Table: %" PRIsVALUE, arrow_table);
    }
    auto garrow_table = RVAL2GOBJ(arrow_table);
    const idx_t rows_per_tuple = 1000000;
    reinterpret_cast<duckdb::Connection *>(ctx->con)
      ->TableFunction(
        "arrow_scan",
        {
          duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(garrow_table)),
          duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(arrow_table_produce)),
          duckdb::Value::UBIGINT(rows_per_tuple)
        })
      ->CreateView(c_name, true, true);

    auto arrow_tables = rb_iv_get(self, "@arrow_tables");
    if (NIL_P(arrow_tables)) {
      arrow_tables = rb_hash_new();
      rb_iv_set(self, "@arrow_tables", arrow_tables);
    }
    rb_hash_aset(arrow_tables, name, arrow_table);

    if (rb_block_given_p()) {
      QueryRegisterArrowData data;
      data.self = self;
      data.name = name;
      return rb_ensure(query_register_arrow_body,
                       Qnil,
                       query_register_arrow_ensure,
                       reinterpret_cast<VALUE>(&data));
    } else {
      return self;
    }
  }

  void init()
  {
    cArrowTable = rb_const_get(rb_const_get(rb_cObject, rb_intern("Arrow")),
                               rb_intern("Table"));

    VALUE mArrowDuckDB;
    mArrowDuckDB = rb_define_module("ArrowDuckDB");
    cArrowDuckDBResult = rb_define_class_under(mArrowDuckDB,
                                               "Result",
                                               rb_cObject);
    rb_define_alloc_func(cArrowDuckDBResult, result_alloc_func);
    rb_define_method(cArrowDuckDBResult, "fetch", result_fetch, 0);

    rb_define_method(cDuckDBConnection, "query_sql_arrow", query_sql_arrow, 1);
    rb_define_method(cDuckDBConnection,
                     "register_arrow",
                     query_register_arrow,
                     2);
    rb_define_method(cDuckDBConnection,
                     "unregister_arrow",
                     query_unregister_arrow,
                     1);
  }
}

extern "C" void
Init_arrow_duckdb(void)
{
  init();
}
