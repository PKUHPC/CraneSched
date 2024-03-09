add_executable(predictor
        Predictor.h
        Predictor.cpp)

target_link_libraries(predictor PRIVATE crane_proto_lib)