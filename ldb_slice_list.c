#include "ldb_slice_list.h"
#include "ldb_slice.h"
#include "lmalloc.h"

#include <string.h>


ldb_slice_node_t* ldb_slice_node_create(){
  ldb_slice_node_t *node = lmalloc(sizeof(ldb_slice_node_t));
  if(node == NULL ){
    return NULL;
  }
  node->next_ = NULL;
  node->prev_ = NULL;
  node->slice_ = NULL;
  return node;
}

void ldb_slice_node_destroy(ldb_slice_node_t* node){
  ldb_slice_destroy(node->slice_);
  lfree(node);
}

ldb_slice_list_t* ldb_slice_list_create(){
  ldb_slice_list_t *list = lmalloc(sizeof(ldb_slice_list_t));
  if(list == NULL){
    return NULL;
  }
  list->head_ = NULL;
  list->tail_ = NULL;
  list->length_ = 0;
  return list;
}

void ldb_slice_list_destroy(ldb_slice_list_t* list){
  if(list != NULL){
    ldb_slice_node_t *node=NULL;
    while(list->head_!=NULL){
      node = list->head_->next_;
      ldb_slice_node_destroy(list->head_);
      list->length_ -= 1;
      list->head_ = node;
    }
    lfree(list);
  }
}

size_t rpush_ldb_slice_node(ldb_slice_list_t* list, ldb_slice_node_t* node){
  if(list == NULL){
    return 0;
  }
  if(node == NULL){
    return list->length_;
  }
  if(list->head_!=NULL){
    list->tail_->next_ = node;
    node->prev_ = list->tail_;
    list->tail_ = node;
  }else{
    list->tail_ = node;
    list->head_ = node;
  }
  list->length_ += 1;
  return list->length_;
}

size_t lpush_ldb_slice_node(ldb_slice_list_t* list, ldb_slice_node_t* node){
  if( list == NULL ){
    return 0;
  }
  if( node == NULL ){
    return list->length_;
  }
  if( list->head_ != NULL){
    list->head_->prev_ = node;
    node->next_ = list->head_;
    list->head_ = node;
  }else{
    list->head_ = node;
    list->tail_ = node;
  }
  list->length_ += 1;
  return list->length_;
}

static void remove_ldb_slice_node(ldb_slice_node_t* node){
  if(node != NULL){
    if(node->prev_!=NULL){
      node->prev_->next_ = node->next_;
    }
    if(node->next_!=NULL){
      node->next_->prev_ = node->prev_;
    }
  }
}

ldb_slice_node_t* rpop_ldb_slice_node(ldb_slice_list_t* list){
  if(list == NULL){
    return NULL;
  }
  remove_ldb_slice_node(list->head_);
  ldb_slice_node_t* node = list->head_;
  list->head_ = list->head_->next_;
  list->length_ -= 1;
  return node;
}

ldb_slice_node_t* lpop_ldb_slice_node(ldb_slice_list_t* list){
  if(list == NULL){
    return NULL;
  }
  if(list->length_==0){
    return NULL;
  }
  remove_ldb_slice_node(list->tail_);
  ldb_slice_node_t* node = list->tail_;
  list->tail_ = list->tail_->prev_;
  list->length_ -= 1;
  return node;
}
