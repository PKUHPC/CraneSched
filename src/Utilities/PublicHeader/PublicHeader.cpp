#include "crane/PublicHeader.h"

AllocatableResource& AllocatableResource::operator+=(
    const AllocatableResource& rhs) {
  cpu_count += rhs.cpu_count;
  memory_bytes += rhs.memory_bytes;
  memory_sw_bytes += rhs.memory_sw_bytes;
  return *this;
}

AllocatableResource& AllocatableResource::operator-=(
    const AllocatableResource& rhs) {
  cpu_count -= rhs.cpu_count;
  memory_bytes -= rhs.memory_bytes;
  memory_sw_bytes -= rhs.memory_sw_bytes;
  return *this;
}

bool operator<=(const AllocatableResource& lhs,
                const AllocatableResource& rhs) {
  if (lhs.cpu_count <= rhs.cpu_count && lhs.memory_bytes <= rhs.memory_bytes &&
      lhs.memory_sw_bytes <= rhs.memory_sw_bytes)
    return true;

  return false;
}

bool operator<(const AllocatableResource& lhs, const AllocatableResource& rhs) {
  if (lhs.cpu_count < rhs.cpu_count && lhs.memory_bytes < rhs.memory_bytes &&
      lhs.memory_sw_bytes < rhs.memory_sw_bytes)
    return true;

  return false;
}

bool operator==(const AllocatableResource& lhs,
                const AllocatableResource& rhs) {
  if (lhs.cpu_count == rhs.cpu_count && lhs.memory_bytes == rhs.memory_bytes &&
      lhs.memory_sw_bytes == rhs.memory_sw_bytes)
    return true;

  return false;
}

bool operator<=(const DedicatedResource& lhs, const DedicatedResource& rhs){
  return lhs.nvidia_graphics_card<=rhs.nvidia_graphics_card &&
    rhs.nvidia_graphics_card == (rhs.nvidia_graphics_card & lhs.nvidia_graphics_card);
}
bool operator<(const DedicatedResource& lhs, const DedicatedResource& rhs){
  return lhs.nvidia_graphics_card < rhs.nvidia_graphics_card &&
    rhs.nvidia_graphics_card == (rhs.nvidia_graphics_card & lhs.nvidia_graphics_card);
}

bool operator==(const DedicatedResource& lhs,const DedicatedResource& rhs){
  return lhs.nvidia_graphics_card==rhs.nvidia_graphics_card;
}

AllocatableResource::AllocatableResource(
    const crane::grpc::AllocatableResource& value) {
  cpu_count = value.cpu_core_limit();
  memory_bytes = value.memory_limit_bytes();
  memory_sw_bytes = value.memory_sw_limit_bytes();
}

AllocatableResource& AllocatableResource::operator=(
    const crane::grpc::AllocatableResource& value) {
  cpu_count = value.cpu_core_limit();
  memory_bytes = value.memory_limit_bytes();
  memory_sw_bytes = value.memory_sw_limit_bytes();
  return *this;
}

Resources& Resources::operator+=(const Resources& rhs) {
  allocatable_resource += rhs.allocatable_resource;
  return *this;
}

Resources& Resources::operator-=(const Resources& rhs) {
  allocatable_resource -= rhs.allocatable_resource;
  return *this;
}

Resources& Resources::operator+=(const AllocatableResource& rhs) {
  allocatable_resource += rhs;
  return *this;
}

Resources& Resources::operator-=(const AllocatableResource& rhs) {
  allocatable_resource -= rhs;
  return *this;
}

bool operator<=(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource <= rhs.allocatable_resource && lhs.delicated_resource <= rhs.delicated_resource;
}

bool operator<(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource < rhs.allocatable_resource && lhs.delicated_resource < rhs.delicated_resource;
}

bool operator==(const Resources& lhs, const Resources& rhs) {
  return lhs.allocatable_resource == rhs.allocatable_resource && lhs.delicated_resource == rhs.delicated_resource;
}

DedicatedResource& DedicatedResource::operator+=(const DedicatedResource& rhs){
  this->nvidia_graphics_card += rhs.nvidia_graphics_card;
  return *this;
}
DedicatedResource& DedicatedResource::operator-=(const DedicatedResource& rhs){
  this->nvidia_graphics_card -= rhs.nvidia_graphics_card;
  return *this;
}