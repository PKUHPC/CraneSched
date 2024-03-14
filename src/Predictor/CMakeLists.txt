include(FetchContent)

FetchContent_Declare(
        xgboost
        GIT_REPOSITORY https://github.com/dmlc/xgboost
        GIT_TAG v2.0.3
        GIT_SHALLOW TRUE
        UPDATE_DISCONNECTED TRUE
)

FetchContent_MakeAvailable(xgboost)

add_executable(predictor
        Predictor.cpp
        TimeEstimator.h
        PredictorPreCompiledHeader.h
        PredictorPublicDefs.h
        PredGrpcServer.cpp
        PredGrpcServer.h
        BaselineEstimator.cpp
        BaselineEstimator.h
        XGBoostEstimator.cpp
        XGBoostEstimator.h
)

target_link_libraries(predictor PRIVATE
        crane_proto_lib
        Utility_PublicHeader
        xgboost)