include(FetchContent)

FetchContent_Declare(
        lightgbm
        GIT_REPOSITORY https://github.com/microsoft/LightGBM
        GIT_TAG v4.3.0
        GIT_SHALLOW TRUE
        UPDATE_DISCONNECTED TRUE
)

FetchContent_MakeAvailable(lightgbm)

FetchContent_GetProperties(lightgbm)
if(NOT lightgbm_POPULATED)
    FetchContent_Populate(lightgbm)
    add_subdirectory(${lightgbm_SOURCE_DIR} ${lightgbm_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

add_executable(predictor
        Predictor.cpp
        TimeEstimator.h
        PredictorPreCompiledHeader.h
        PredictorPublicDefs.h
        PredGrpcServer.cpp
        PredGrpcServer.h
        BaselineEstimator.cpp
        BaselineEstimator.h
        LightGBMEstimator.cpp
        LightGBMEstimator.h
)

target_include_directories(predictor PRIVATE ${lightgbm_SOURCE_DIR}/include)

target_link_libraries(predictor PRIVATE
        crane_proto_lib
        Utility_PublicHeader
        _lightgbm)