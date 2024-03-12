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
        Utility_PublicHeader)