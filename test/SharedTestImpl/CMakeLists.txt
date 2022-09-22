add_library(shared_test_impl_lib greeter_service_impl.cpp
        include/SharedTestImpl/GlobalDefs.h include/SharedTestImpl/greeter_service_impl.h)

target_link_libraries(shared_test_impl_lib PUBLIC test_proto Utility_PublicHeader)
target_include_directories(shared_test_impl_lib PUBLIC include)

