/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef SRC_AVRO_ERRORRESULT_HH_4090747368__H_
#define SRC_AVRO_ERRORRESULT_HH_4090747368__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace c {
struct ErrorResult {
    int32_t code;
    std::string message;
    ErrorResult() :
        code(int32_t()),
        message(std::string())
        { }
};

}
namespace avro {
template<> struct codec_traits<c::ErrorResult> {
    static void encode(Encoder& e, const c::ErrorResult& v) {
        avro::encode(e, v.code);
        avro::encode(e, v.message);
    }
    static void decode(Decoder& d, c::ErrorResult& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.code);
                    break;
                case 1:
                    avro::decode(d, v.message);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.code);
            avro::decode(d, v.message);
        }
    }
};

}
#endif