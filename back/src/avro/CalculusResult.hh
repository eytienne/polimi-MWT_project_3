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


#ifndef SRC_AVRO_CALCULUSRESULT_HH_3759926484__H_
#define SRC_AVRO_CALCULUSRESULT_HH_3759926484__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace c {
struct CalculusResult {
    std::string expression;
    CalculusResult() :
        expression(std::string())
        { }
};

}
namespace avro {
template<> struct codec_traits<c::CalculusResult> {
    static void encode(Encoder& e, const c::CalculusResult& v) {
        avro::encode(e, v.expression);
    }
    static void decode(Decoder& d, c::CalculusResult& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.expression);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.expression);
        }
    }
};

}
#endif
