/* -*- coding: utf-8 -*-
 * vim:fenc=utf-8
 *
 * Copyright © Her Majesty the Queen in Right of Canada, as represented
 * by the Minister of Statistics Canada, 2019.
 *
 * Distributed under terms of the license.
 */

#include <fwfr/test_reader.h>

namespace fwfr {
namespace test {

std::shared_ptr<arrow::Table> test_read (const char* fname, 
                                        std::vector<uint32_t> field_widths,
                                        std::string encoding, bool verbose) {   
    // Setup
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::Table> table;

    // Get input stream
    std::shared_ptr<arrow::io::ReadableFile> file;
    std::shared_ptr<arrow::io::InputStream> fstream;
    
    arrow::Status st = arrow::io::ReadableFile::Open(fname, pool, &file);
    if (!st.ok()) {
        std::cerr << st.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }
    fstream = file;

    // Configure TableReader options
    auto read_options = arrow::fwfr::ReadOptions::Defaults();
    read_options.encoding = encoding;

    auto parse_options = arrow::fwfr::ParseOptions::Defaults();
    parse_options.field_widths = field_widths;
    
    // Generate the table
    std::shared_ptr<arrow::fwfr::TableReader> reader;
    st = arrow::fwfr::TableReader::Make(pool, fstream,
                                        read_options,
                                        parse_options,
                                        arrow::fwfr::ConvertOptions::Defaults(),
                                        &reader);

    if (!st.ok()) {
        std::cerr << st.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }

    st = reader->Read(&table);
    if (!st.ok()) {
        std::cerr << st.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }
    std::cout << "Table generated successfully." << std::endl;

    // Read the table
    if (verbose) {
        std::shared_ptr<arrow::Column> tmp_col;
        for (int i=0; i < table->num_columns(); i++) {
            tmp_col = table->column(i);
            std::cout << tmp_col->name() << ":\t";
            std::cout << "records: " << tmp_col->length() << ", ";
            std::cout << "datatype: " << *(tmp_col->type()) << ", ";
            std::cout << "nullcount: " << tmp_col->null_count() << ", ";
            std::cout << "data: " << *(tmp_col->data()->chunk(0)) << std::endl;
        }
    }
    return table; 
}

}  // namespace test
}  // namespace fwfr
