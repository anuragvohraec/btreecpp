
#include <exception>
#include <memory>
#include <sys/types.h>
#include <sys/uio.h>
#include <functional>
#include <vector>

#ifndef BTREE
#define BTREE

template<typename K>
using ComparatorFunction = std::function<int(std::shared_ptr<K> k1,std::shared_ptr<K> k2)>;


template<typename K>
struct LinkedNode{
  std::shared_ptr<LinkedNode<K>> rightSibling;  
  std::shared_ptr<LinkedNode<K>> leftSibling;  

  int duplicate_count=0;
  std::shared_ptr<K> key;

  LinkedNode(std::shared_ptr<K> key):key(key){

  }
};

template<typename K>
struct SortedLinkedList{
    std::shared_ptr<LinkedNode<K>> min;
    std::shared_ptr<LinkedNode<K>> max;
    int count=0;
};

template<typename K>
struct BPlusTree;

template<typename K>
struct BPlusCell;

template<typename K>
struct BPlusNode{
    std::weak_ptr<BPlusCell<K>> parent_cell;

    std::weak_ptr<BPlusNode<K>> rightSibling;
    std::weak_ptr<BPlusNode<K>> leftSibling;

    std::weak_ptr<BPlusNode<K>> parent_node;

    std::shared_ptr<SortedLinkedList<BPlusCell<K>>> cellsList;

    std::weak_ptr<BPlusTree<K>>parent_tree;

    std::shared_ptr<BPlusNode<K>> left_most_child;

    bool isLeftMostNode(){
        //there is no parent cell for left most node
        return this->parent_cell.lock()==NULL;
    }

    bool isRoot(){
        return this->parent_node.lock()==NULL;
    }

    int size(){
        return this->cellsList->count;
    }

    bool isLeaf;


    

};

template<typename K>
static void setAsCellsList(std::shared_ptr<BPlusNode<K>> node,std::shared_ptr<SortedLinkedList<BPlusCell<K>>> newCellsList){
    if(!node->isLeaf){
        auto currentLinkedNode = newCellsList->min;
        while(currentLinkedNode){
            if(currentLinkedNode->key->right_child_node)
                currentLinkedNode->key->right_child_node->parent_node=node;
            currentLinkedNode=currentLinkedNode->rightSibling;
        }
    }
    node->cellsList=newCellsList;
}

template<typename K>
static void reinforceParentShipInChildNodes(std::shared_ptr<BPlusNode<K>> node){
    if(!node->isLeaf && node->cellsList){
    auto currentLinkedNode = node->cellsList->min;
    while(currentLinkedNode){
        if(currentLinkedNode->key->right_child_node)
            currentLinkedNode->key->right_child_node->parent_node=node;
        currentLinkedNode=currentLinkedNode->rightSibling;
    }
    }
}

template<typename K>    
static void setAsLeftMostChildNode(std::shared_ptr<BPlusNode<K>> node,std::shared_ptr<BPlusNode<K>> nodeToSetAsLeftMostChild){
    node->left_most_child=nodeToSetAsLeftMostChild;
    nodeToSetAsLeftMostChild->parent_node=node;
    nodeToSetAsLeftMostChild->parent_cell.lock()=NULL;
}

template<typename K>
static std::shared_ptr<BPlusNode<K>> createBPlusNode(std::shared_ptr<BPlusTree<K>>parent_tree,bool isLeaf,std::shared_ptr<BPlusNode<K>> parent_node=NULL, std::shared_ptr<BPlusNode<K>> left_most_child=NULL){
    std::shared_ptr<BPlusNode<K>> t(new BPlusNode<K>());
    t->parent_tree=parent_tree;
    t->isLeaf=isLeaf;
    t->parent_node=parent_node;
    t->left_most_child=left_most_child;

    t->cellsList=std::shared_ptr<SortedLinkedList<BPlusCell<K>>>(new SortedLinkedList<BPlusCell<K>>());
    if(t->left_most_child){
        t->left_most_child->parent_node=t;
        t->left_most_child->parent_cell.lock()=NULL;
    }

    return t;
}


template<typename K>
struct BPlusCell{
    std::shared_ptr<K> key;
    std::shared_ptr<void> value;

    std::shared_ptr<BPlusNode<K>> right_child_node;
    std::weak_ptr<BPlusNode<K>> parentNodeForRightChildNode;

    void setAsRightChildNode(std::shared_ptr<BPlusNode<K>> nodeToBecomeRCNOfThisCell){
        std::shared_ptr<BPlusNode<K>> existingParentNode;
        if(this->right_child_node){
            existingParentNode=this->right_child_node->parent_node;
        }

        this->right_child_node=nodeToBecomeRCNOfThisCell;
        nodeToBecomeRCNOfThisCell->parent_cell=this;
        nodeToBecomeRCNOfThisCell->parent_node=existingParentNode;
    }
};

template<typename K>
static std::shared_ptr<BPlusCell<K>> createBPlusCell(std::shared_ptr<K> key,std::shared_ptr<BPlusNode<K>> right_child_node=NULL,std::shared_ptr<BPlusNode<K>> parentNodeForRightChildNode=NULL){
    //:key(key),right_child_node(right_child_node),parentNodeForRightChildNode(parentNodeForRightChildNode)
    std::shared_ptr<BPlusCell<K>> t(new BPlusCell<K>);
    t->key=key;
    t->right_child_node=right_child_node;
    t->parentNodeForRightChildNode=parentNodeForRightChildNode;

    if(t->right_child_node){
        t->right_child_node->parent_cell=t;
        
        if(t->right_child_node->parent_node.lock())
            t->right_child_node->parent_node.lock()=parentNodeForRightChildNode;
    }

    return t;
}



template<typename K>
struct BPlusTree{
    std::shared_ptr<BPlusNode<K>> left_most_node;
    std::shared_ptr<BPlusNode<K>> right_most_node;
    std::shared_ptr<BPlusNode<K>> root_node;

    uint64_t size=0;
    int half_capacity=0;
    int max_node_size=4;

    BPlusTree(int max_node_size):max_node_size(max_node_size){
    if(max_node_size%2==1){
      throw "${Const.BalancedTrees} : node_size for tree must be an even number";
    }
    this->half_capacity= this->max_node_size/2;
    }
};


enum SearchType{
  LesserThanOrEqualsTo,
  EqualsTo,
  GreaterThanOrEqualsTo
};

namespace LL {
    template<typename K>
    static std::shared_ptr<LinkedNode<K>> search(std::shared_ptr<SortedLinkedList<K>> list, ComparatorFunction<K> compare,std::shared_ptr<K> searchKey, SearchType searchType=SearchType::EqualsTo){
        if(list->min==nullptr){
            return list->min;
        }else{
            std::shared_ptr<LinkedNode<K>> eq;
            std::shared_ptr<LinkedNode<K>> lte;
            std::shared_ptr<LinkedNode<K>> gte;

            std::shared_ptr<LinkedNode<K>> current_node = list->min;
            while (current_node) {
                auto compareResult = compare(searchKey, current_node->key);
                if (compareResult == 0) {
                //found an exact match;
                eq = current_node;
                break;
                } else if (compareResult > 0) {
                //search key is bigger than current
                lte = current_node;
                current_node = current_node->rightSibling;
                } else {
                //search key is smaller than current
                gte = current_node;
                break;
                }
            }

            switch (searchType) {
                case SearchType::LesserThanOrEqualsTo: return eq? eq : lte;
                case SearchType::EqualsTo: return eq;
                case SearchType::GreaterThanOrEqualsTo: return eq? eq : gte;
            }
        }
        return NULL;
    }
    template<typename K>
    static std::shared_ptr<LinkedNode<K>> insert(std::shared_ptr<SortedLinkedList<K>> list, ComparatorFunction<K> compare,std::shared_ptr<K> key){
        std::shared_ptr<LinkedNode<K>> newNode(new LinkedNode<K>(key));

        if(list->min==nullptr){
            list->min=newNode;
            list->max=newNode;
        }else{
            auto foundNode = search(list,compare, key, SearchType::LesserThanOrEqualsTo);
            //as this is a less than or equals to search, if found node is undefined
            //then its less than minimum of the list
            if(!foundNode){
                //new node is new min
                list->min->leftSibling=newNode;
                newNode->rightSibling=list->min;
                list->min=newNode;
            }else{
                if(compare(key, foundNode->key)==0){
                    foundNode->duplicate_count++;
                    list->count--;
                    //this is done as change feeds in recliner db were failing because of this.
                    foundNode->key=newNode->key;
                    newNode=foundNode;
                }else{
                    //found node is less then new node, that means its left sibling of new node
                    auto right_sibling = foundNode->rightSibling;
                    foundNode->rightSibling=newNode;
                    newNode->leftSibling=foundNode;

                    newNode->rightSibling=right_sibling;
                    if(right_sibling){
                        right_sibling->leftSibling=newNode;
                    }
                    
                    if(foundNode==list->max){
                        list->max=newNode;
                    }
                }
            }
        }

        list->count++;

        return newNode;
    }

    template<typename K>
    static std::shared_ptr<LinkedNode<K>> deleteNode(std::shared_ptr<SortedLinkedList<K>> list, ComparatorFunction<K> compare,std::shared_ptr<K> key){
        auto nodeToDelete= LL::search(list,compare, key, SearchType::EqualsTo);
        if(nodeToDelete){
            std::shared_ptr<LinkedNode<K>> deletedNode(new LinkedNode<K>(nodeToDelete->key));
            deletedNode->duplicate_count=nodeToDelete->duplicate_count;

            if(list->count==1){
                list->min=NULL;
                list->max=NULL;
            }else{
                // assert(list->count>=2);
                if(nodeToDelete==list->min){
                    auto oldMin=list->min;
                    list->min=nodeToDelete->rightSibling;
                    
                    oldMin->rightSibling->leftSibling=NULL;
                    oldMin->rightSibling=NULL;
                }else if(nodeToDelete==list->max){
                    auto oldMax=list->max;
                    list->max=nodeToDelete->leftSibling;

                    oldMax->leftSibling->rightSibling=NULL;
                    oldMax->leftSibling=NULL;
                }
                auto left_sibling = nodeToDelete->leftSibling;
                auto right_sibling = nodeToDelete->rightSibling;
                
                if(left_sibling){
                    left_sibling->rightSibling=right_sibling;
                }
                
                if(right_sibling){
                    right_sibling->leftSibling=left_sibling;
                }
                
                nodeToDelete->leftSibling=NULL;
                nodeToDelete->rightSibling=NULL;
                nodeToDelete->key=NULL;
            }
            list->count-=(deletedNode->duplicate_count+1);
            return deletedNode;
        }
        return NULL;
    }

    /**
    returns a splitted_lists of size 2, splitted_lists[[0]] is left portion, splitted_lists[[1]] is right portion.
  
    **listToSplit**: its size must be at least of length 2.
  
    **splitAfterIndex**: Items till splitAfterIndex are in left portion
    */
    template<typename K>
    static std::shared_ptr<std::vector<std::shared_ptr<SortedLinkedList<K>>>>  splitAt(std::shared_ptr<SortedLinkedList<K>> listToSplit , int splitAfterIndex){
        std::shared_ptr<std::vector<std::shared_ptr<SortedLinkedList<K>>>> result(new std::vector<std::shared_ptr<SortedLinkedList<K>>>);

        if(listToSplit->count<2){
            throw "${Const.BalancedTrees} : Size of List to be split must be atleast 2";
        }

        if(!(splitAfterIndex<listToSplit->count-1)){
            if(splitAfterIndex==listToSplit->count-1){
                result->push_back(listToSplit);
                result->push_back(std::shared_ptr<SortedLinkedList<K>>(new SortedLinkedList<K>()));
                return result;
            }
            throw "${Const->BalancedTrees}: splitAtIndex must be less than listToSplit->count-1";
        }
        auto origSize = listToSplit->count;

        //LinkedNode<K> minOfLeftPortion=listToSplit->min;//not required
        auto maxOfRightPortion=listToSplit->max;

        auto minOfRightPortion=listToSplit->min;
        for(int i=0;i<=splitAfterIndex;i++){
            if(minOfRightPortion){
                minOfRightPortion=minOfRightPortion->rightSibling;
            }
        }

        std::shared_ptr<LinkedNode<K>> maxOfLeftPortion;
        if(minOfRightPortion){
            maxOfLeftPortion = minOfRightPortion->leftSibling;
        }

        //lets split
        if(maxOfLeftPortion)
            maxOfLeftPortion->rightSibling=NULL;

        if(minOfRightPortion)
            minOfRightPortion->leftSibling=NULL;

        //setting up left
        listToSplit->count=splitAfterIndex+1;
        listToSplit->max=maxOfLeftPortion;

        //setting up right portion;
        std::shared_ptr<SortedLinkedList<K>> rightPortion(new SortedLinkedList<K>());
        rightPortion->min=minOfRightPortion;
        rightPortion->max=maxOfRightPortion;
        rightPortion->count=origSize-splitAfterIndex-1;

        result->push_back(listToSplit);
        result->push_back(rightPortion);
        return result;
    }

    /*
    merges rightlist into leftlist, rightlist keys are always bigger than leftlist, no check is performed inside.
    Its something user must remember, if they want to preserve the sorted list sorting
    return leftlist,after merge
    */
    template<typename K>
    static std::shared_ptr<SortedLinkedList<K>> mergeSplittedRightIntoLeft( std::shared_ptr<SortedLinkedList<K>> leftlist,std::shared_ptr<SortedLinkedList<K>> rightlist){
        if(rightlist->count==0){
            return leftlist;
        }
        auto maxOfRight = rightlist->max;
        auto countOfRight = rightlist->count;

        //merging
        if(leftlist->max)
            leftlist->max->rightSibling=rightlist->min;
        if(rightlist->min)
            rightlist->min->leftSibling=leftlist->max;

        leftlist->max= maxOfRight;
        leftlist->count=leftlist->count+countOfRight;

        return leftlist;
    }

    /*
    merges rightlist into leftlist, rightlist keys are always bigger than leftlist, no check is performed inside.
    Its something user must remember, if they want to preserve the sorted list sorting
    return rightlist,after merge
    */
    template<typename K>
    static std::shared_ptr<SortedLinkedList<K>> mergeSplittedLeftIntoRight(std::shared_ptr<SortedLinkedList<K>> leftlist,std::shared_ptr<SortedLinkedList<K>>  rightlist){
        if(leftlist->count==0){
            return rightlist;
        }
        auto minOfLeft = leftlist->min;
        auto countOfLeft = leftlist->count;

        //merging
        if(leftlist->max)
            leftlist->max->rightSibling=rightlist->min;
        if(rightlist->min)
            rightlist->min->leftSibling=leftlist->max;

        rightlist->min= minOfLeft;
        rightlist->count=rightlist->count+countOfLeft;

        return rightlist;
    }


    /*
    If search type is LesserThanOrEqual, than if no equal or less than is found , it will simply end stream.
    if search type is GreaterThanOrEqual, than if no equal or greater than is found than it will simply end the stream.
    It will Stream nodes until nodes are smaller than or equals to till key. If till key is undefined , then it will stream all the right-siblings till the end
    */
    template<typename K>
    static  std::shared_ptr<std::vector<std::shared_ptr<K>>> searchTillStream( std::shared_ptr<SortedLinkedList<K>> list, ComparatorFunction<K> compare, std::shared_ptr<K> startKey=NULL,std::shared_ptr<K> endKey=NULL,bool yieldIndividualDuplicates=false){
        std::shared_ptr<std::vector<std::shared_ptr<K>>> result(new std::vector<std::shared_ptr<K>>());
        
        std::shared_ptr<LinkedNode<K>> currentNode = !startKey?list->min:LL::search(list, compare, startKey, SearchType::GreaterThanOrEqualsTo);
        std::shared_ptr<LinkedNode<K>> endNode = !endKey?list->max:LL::search(list, compare, endKey,SearchType::LesserThanOrEqualsTo);

        if(endNode){
            while(currentNode){
                auto cr = compare(endNode->key,currentNode->key);
                if(cr>=0){
                    std::shared_ptr<LinkedNode<K>> res= currentNode;
                    currentNode=currentNode->rightSibling;
                    if(yieldIndividualDuplicates){
                        auto countOfDuplicates=res->duplicate_count+1;
                        for(int i=0;i<countOfDuplicates;i++){
                            result->push_back(res->key);
                        }
                    }else{
                        result->push_back(res->key);
                    }
                }else{
                    currentNode=NULL;
                }
            }
        }

        return result;
    }

    template<typename K>
    static  std::shared_ptr<std::vector<std::shared_ptr<K>>> find( std::shared_ptr<SortedLinkedList<K>> list, ComparatorFunction<K> queryCompare,bool yieldIndividualDuplicates=false){
        std::shared_ptr<std::vector<std::shared_ptr<K>>> result(new std::vector<std::shared_ptr<K>>());
        auto cn = list->min;
        while(cn){
            if(queryCompare(cn->key,cn->key)==0){
                result->push_back(cn->key);
            }
            cn=cn->rightSibling;
        }
        return result;
    }
}

namespace BB {
    enum BalanceCase{
        DO_NOTHING,

        REMOVE_ROOT,

        SPLIT,

        DISTRIBUTE_RIGHT_INTO_NODE,
        DISTRIBUTE_LEFT_INTO_NODE,

        MERGE_RIGHT_INTO_NODE,
        MERGE_NODE_INTO_LEFT,
    };

    enum SOURCE_IS{
    LEFT_SIBLING, RIGHT_SIBLING
    };


    template<typename K>
    static BalanceCase _determineBalancingCase( std::shared_ptr<BPlusTree<K>> tree , std::shared_ptr<BPlusNode<K>> effectedNode ){
        auto node_size=effectedNode->size();
        auto half_capacity = tree->half_capacity;

        if(half_capacity<=node_size && node_size<=tree->max_node_size){
            return BalanceCase::DO_NOTHING;
        }else{
            if(node_size>tree->max_node_size){
            return BalanceCase::SPLIT;
            }else{
            //this node size < half capacity

            //we give prereference too distribution first, that too to right node for distribution
            auto left_sibling_size = 0;
            if(effectedNode->leftSibling.lock()){
                left_sibling_size=effectedNode->leftSibling.lock()->size();
            }
            auto right_sibling_size = 0;
            if(effectedNode->rightSibling.lock()){
                right_sibling_size=effectedNode->rightSibling.lock()->size();
            }

            //case of root node
            if(left_sibling_size==0 && right_sibling_size==0){
                if(effectedNode->size()==0){
                return BalanceCase::REMOVE_ROOT;
                }else{
                return BalanceCase::DO_NOTHING;
                }
            }

            if(right_sibling_size>half_capacity){
                return BalanceCase::DISTRIBUTE_RIGHT_INTO_NODE;
            }

            if(left_sibling_size>half_capacity){
                return BalanceCase::DISTRIBUTE_LEFT_INTO_NODE;
            }

            if(right_sibling_size>0){
                return BalanceCase::MERGE_RIGHT_INTO_NODE;
            }

            if(left_sibling_size>0){
                return BalanceCase::MERGE_NODE_INTO_LEFT;
            }

            throw "${Const.BalancedTrees}: NO BALANCE CASE found for: $effectedNode";
            }
        }
    }

    template<typename K>
    static std::shared_ptr<BPlusCell<K>> find_effective_parent_cell(std::shared_ptr<BPlusNode<K> >effectiveNode){
        auto effective_parent_cell=effectiveNode->parent_cell;
        if(!effective_parent_cell.lock()){
        auto currentNode= effectiveNode;
        while(currentNode){
            effective_parent_cell = currentNode->parent_cell;
            if(effective_parent_cell.lock()){
            return effective_parent_cell.lock();
            }
            currentNode=currentNode->parent_node.lock();
        }

        if(!effective_parent_cell.lock() && effectiveNode->parent_node.lock() && effectiveNode->parent_node.lock()->cellsList && effectiveNode->parent_node.lock()->cellsList->min){
            return effectiveNode->parent_node.lock()->cellsList->min->key;
        }else{
            throw "Not allowed condition";
        }
        }else{
            return effective_parent_cell.lock();
        }
    }

    template<typename K>
    static std::shared_ptr<BPlusNode<K>> split(std::shared_ptr<BPlusTree<K>> tree , std::shared_ptr<BPlusNode<K>> effectedNode ,ComparatorFunction<BPlusCell<K>> customCompare) {
        //its assumed that effected node size is greater than node_size, as that check must have been done before calling this

        //Algorithm:
        //Splitting cells list: left node has more items, then right
        //creating new right node and setting its relationships
        //if effected node is root, than create a new root and set it as parent of these two splitted nodes
        //creating parent cell for new right node from maximum value from left node and push parent cell to parent node
        //if effected node is leaf check if it is also right most child, then we will need set that too for tree as new Right Node
        //if its not leaf, then remove the min from right node

        //Splitting cellslist
        auto splitAfterIndex=tree->half_capacity;
        auto splits= LL::splitAt<BPlusCell<K>>(effectedNode->cellsList, splitAfterIndex);

        auto newRightList = (*splits)[1];
        auto newLeftList= effectedNode->cellsList;


        //creating new right node and setting its relationships
        //sets parent and child relation during construction itself
        auto splitRightNode = createBPlusNode<K>(tree, effectedNode->isLeaf, effectedNode->parent_node.lock(), newLeftList->max->key->right_child_node);

        //setting up new right node
        {
        //setting cellsList
        setAsCellsList(splitRightNode,newRightList);

        //setting up sibling relationships
            {
                splitRightNode->rightSibling = effectedNode->rightSibling;
                if(effectedNode->rightSibling.lock()){
                    effectedNode->rightSibling.lock()->leftSibling.lock() = splitRightNode;
                }
                
                splitRightNode->leftSibling = effectedNode;
                effectedNode->rightSibling = splitRightNode;
            }
        }

        //if effected node is root, than create a new root
        if (effectedNode->isRoot()) {
            auto newRootNode = createBPlusNode<K>(tree, false, NULL, effectedNode);
            tree->root_node = newRootNode;
        }

        //definitely have a parent node
        auto parent_node = effectedNode->parent_node;
        // assert(parent_node!=undefined);
        splitRightNode->parent_node=parent_node;//this case is required to tackle root node split

        //creating parent cell for new right node from maximum value from left node and push parent cell to parent node
        {
        auto parentCellForNewRightNode=createBPlusCell<K>(newLeftList->max->key->key,splitRightNode,parent_node.lock());
        LL::insert(parent_node.lock()->cellsList, customCompare, parentCellForNewRightNode);
        }

        //if effected node is leaf
        if (effectedNode->isLeaf) {
        //and also right most child, then we will need set that too for tree as new Right Node
        if(effectedNode == tree->right_most_node){
            tree->right_most_node = splitRightNode;
        }
        } else {
        //if its not leaf, then remove the max from left node, its right child node is already taken care of above
        //see splitRightNode creation we pass it as left most child to right node
        LL::deleteNode(newLeftList, customCompare,createBPlusCell<K>(newLeftList->max->key->key, NULL,NULL));
        }

        return parent_node.lock();
    }

    ///Source is always right sibling
    template<typename K>
    static std::shared_ptr<BPlusNode<K>> merge(
        std::shared_ptr<BPlusTree<K>> tree ,
        std::shared_ptr<BPlusNode<K>> source,
        std::shared_ptr<BPlusNode<K>> target,
        ComparatorFunction<BPlusCell<K>> customCompare){
        //its assumed that source and target size are all calculated before hand, and this is indeed a case of merge

        //1. Find effective parent cell
        auto effectiveNode = source;
        auto effective_parent_cell= BB::find_effective_parent_cell<K>(effectiveNode);

        //2. handling first cell for target
        if(!source->isLeaf){
        auto right_child_node = source->left_most_child;
        auto first_cell_for_target= createBPlusCell<K>(effective_parent_cell->key, right_child_node, target);
        LL::insert(target->cellsList,customCompare, first_cell_for_target);
        }

        //3. Merge source cellsList in target
        LL::mergeSplittedRightIntoLeft(target->cellsList, source->cellsList);
        reinforceParentShipInChildNodes(target);

        //4. set up new siblings relationship, disconnect old sibling relation of source node
        target->rightSibling=source->rightSibling;
        if(source->rightSibling.lock()){
        source->rightSibling.lock()->leftSibling.lock()=target;
        }

        //5. Remove parent relation of sourcenode
        if(source->isLeftMostNode()){
            auto replacement_key= source->parent_node.lock()->cellsList->min->key->key;
            auto deletedN= LL::deleteNode(source->parent_node.lock()->cellsList, customCompare, createBPlusCell<K>(replacement_key, NULL,NULL));
            if(deletedN && deletedN->key){
                auto deletedKey=deletedN->key;
                if(deletedKey->right_child_node)
                setAsLeftMostChildNode(source->parent_node.lock(),deletedKey->right_child_node);   
            }
            effective_parent_cell->key=replacement_key;
        }else{
            LL::deleteNode(source->parent_node.lock()->cellsList, customCompare,createBPlusCell<K>(source->parent_cell.lock()->key));
        }

        if(source==tree->right_most_node){
        tree->right_most_node=target;
        }

        return source->parent_node.lock();
    }

    ///Source will have alays have more nodes than target and more than half capacity, no check performed here. Its must be performed at source end.
    template <typename K>
    static void distribute(std::shared_ptr<BPlusTree<K>> tree  ,std::shared_ptr<BPlusNode<K>>   source,std::shared_ptr<BPlusNode<K>>   target,SOURCE_IS  source_is ,ComparatorFunction<BPlusCell<K>> customCompare){
        //1. Finding effective parent cell
        auto effectiveNode = source_is==SOURCE_IS::LEFT_SIBLING?target:source;
        auto effective_parent_cell= BB::find_effective_parent_cell<K>(effectiveNode);
        
        //2. handling first cell for target
        if(!source->isLeaf){
        if(target->cellsList){
            auto right_child_node = source_is==SOURCE_IS::LEFT_SIBLING?target->left_most_child:source->left_most_child;
            auto first_cell_for_target = createBPlusCell<K>(effective_parent_cell->key, right_child_node, target);
            LL::insert(target->cellsList,customCompare, first_cell_for_target);
        }
        }

        //3. Split cells from source
        
        std::shared_ptr<std::vector<std::shared_ptr<SortedLinkedList<BPlusCell<K>>>>>splitted_cells;
        if(!source->isLeaf){
            if(source_is == SOURCE_IS::RIGHT_SIBLING){
                splitted_cells = LL::splitAt<BPlusCell<K>>(source->cellsList, source->cellsList->count-tree->half_capacity-1);
            }else{
                splitted_cells = LL::splitAt<BPlusCell<K>>(source->cellsList, source->cellsList->count-tree->half_capacity+1);
            }
        }else{
            if(source_is == SOURCE_IS::RIGHT_SIBLING){
                splitted_cells = LL::splitAt<BPlusCell<K>>(source->cellsList, source->cellsList->count-tree->half_capacity-1);
            }else{
                splitted_cells = LL::splitAt<BPlusCell<K>>(source->cellsList, tree->half_capacity-1);
            }

        }

        //4. calculate effective_LMC and replacement_key
        std::shared_ptr<BPlusNode<K>> effective_LMC;
        std::shared_ptr<K> replacement_key;
        switch(source_is){
            case SOURCE_IS::LEFT_SIBLING:{
                auto max_cell_in_source_after_split =source->cellsList->max->key;
                effective_LMC=max_cell_in_source_after_split->right_child_node;
                replacement_key=max_cell_in_source_after_split->key;
                if(!source->isLeaf){
                    LL::deleteNode(source->cellsList,customCompare,createBPlusCell<K>(max_cell_in_source_after_split->key));
                }
            }break;
            case SOURCE_IS::RIGHT_SIBLING:
                setAsCellsList(source,splitted_cells->at(1));
                std::shared_ptr<BPlusCell<K>> max_cell_in_left_portion_after_split;
                if(splitted_cells->at(0)->max){
                    max_cell_in_left_portion_after_split=splitted_cells->at(0)->max->key;
                }
                effective_LMC=max_cell_in_left_portion_after_split->right_child_node;
                replacement_key=max_cell_in_left_portion_after_split->key;
                if(!source->isLeaf){
                    LL::deleteNode(splitted_cells->at(0),customCompare,createBPlusCell<K>(max_cell_in_left_portion_after_split->key));
                }
                break;
        }

        //5. Merge split cells
        switch(source_is){
        case SOURCE_IS::LEFT_SIBLING:
            if(splitted_cells->at(1)->count>0){
            LL::mergeSplittedLeftIntoRight(splitted_cells->at(1), target->cellsList);
            reinforceParentShipInChildNodes(target);
            }
            break;
        case SOURCE_IS::RIGHT_SIBLING:
            if(splitted_cells->at(0)->count>0){
            LL::mergeSplittedRightIntoLeft(target->cellsList, splitted_cells->at(0));
            reinforceParentShipInChildNodes(target);
            }
            break;
        }

        //6. if source **is not leaf**
        //handling the **right_child_node** of **max_cell_in_source_after_split**, making it as LMC:
        if(!source->isLeaf){
        switch(source_is){
            case SOURCE_IS::LEFT_SIBLING:
            setAsLeftMostChildNode(target,effective_LMC);
            break;
            case SOURCE_IS::RIGHT_SIBLING:
            setAsLeftMostChildNode(source,effective_LMC);
            break;
        }
        //SortedLinkedListEngine.delete(source.cellsList!, customCompare, new BPlusCell(replacement_key, undefined, undefined));
        }

        //7. Resetting effective_parent_cell key
        if(source_is == SOURCE_IS::RIGHT_SIBLING && source->isLeaf){
            effective_parent_cell->key=target->cellsList->max->key->key;
        }else{
            effective_parent_cell->key=replacement_key;
        }
    }

    template <typename K>
    static void balance( std::shared_ptr<BPlusTree<K>> tree  ,std::shared_ptr<BPlusNode<K>>  effectedNode, ComparatorFunction<BPlusCell<K>> customCompare){
        try{
            auto balanceCase = _determineBalancingCase(tree, effectedNode);
            switch(balanceCase){
            case BalanceCase::DO_NOTHING:
                break;
            case BalanceCase::REMOVE_ROOT:
                tree->root_node=tree->root_node->left_most_child;
                if(tree->root_node){
                tree->root_node->parent_node.lock()=NULL;
                }
                break;
            case BalanceCase::SPLIT:
                {
                    auto nextnode=BB::split<K>(tree, effectedNode, customCompare);
                    BB::balance<K>(tree, nextnode, customCompare);
                }
                break;
            case BalanceCase::DISTRIBUTE_RIGHT_INTO_NODE:
                BB::distribute<K>(tree, effectedNode->rightSibling.lock(), effectedNode, SOURCE_IS::RIGHT_SIBLING, customCompare);
                break;
            case BalanceCase::DISTRIBUTE_LEFT_INTO_NODE:
                BB::distribute<K>(tree, effectedNode->leftSibling.lock(), effectedNode, SOURCE_IS::LEFT_SIBLING, customCompare);
                break;
            case BalanceCase::MERGE_RIGHT_INTO_NODE:
                {
                    auto nextnode=BB::merge<K>(tree, effectedNode->rightSibling.lock(), effectedNode, customCompare);
                    if(nextnode)
                        BB::balance<K>(tree, nextnode, customCompare);
                }
                break;
            case BalanceCase::MERGE_NODE_INTO_LEFT:
                {
                    auto nextnode=BB::merge<K>(tree, effectedNode, effectedNode->leftSibling.lock(), customCompare);
                    if(nextnode)
                        BB::balance<K>(tree, nextnode, customCompare);
                }
                break;

            }
        }catch(std::exception & e){
            throw "Failed while balancing";
        }
    }

    template<typename K>
    static std::shared_ptr<BPlusNode<K>> searchForLeafNode(std::shared_ptr<BPlusTree<K>> tree, ComparatorFunction<BPlusCell<K>>  compare,std::shared_ptr<K> key,ComparatorFunction<BPlusCell<K>> queryCompare=NULL){
        ComparatorFunction<BPlusCell<K>> effectiveComparator=queryCompare?queryCompare:compare;
        
        if(!tree->root_node){
            return NULL;
        }else{
            std::shared_ptr<BPlusNode<K>> bpNode = tree->root_node;
            auto searchKey=createBPlusCell<K>(key);
            while(bpNode && !bpNode->isLeaf){
                auto foundCell = LL::search(bpNode->cellsList, effectiveComparator, searchKey, SearchType::LesserThanOrEqualsTo);
                if(!foundCell){
                    bpNode=bpNode->left_most_child;
                }else{
                    auto c = effectiveComparator(searchKey,foundCell->key);
                    if(c==0){
                        if(foundCell->leftSibling){
                        bpNode=foundCell->leftSibling->key->right_child_node;
                        }else{
                        bpNode=bpNode->left_most_child;
                        }
                    }else{
                        //assert(c>0);//as we found node must be less than search key here
                        bpNode=foundCell->key->right_child_node;
                    }
                } 
            }
            //bpnode is guaranteed leaf
            return bpNode;
        }
    }


    template<typename K>
    static std::shared_ptr<K> searchForKey( std::shared_ptr<BPlusTree<K>> tree, ComparatorFunction<BPlusCell<K>>  compare,std::shared_ptr<K> searchKey,SearchType searchType = SearchType::EqualsTo){
        auto leafNode = BB::searchForLeafNode(tree, compare, searchKey);
        if(leafNode){
            if(leafNode->cellsList){
                auto foundLinkedNode = LL::search<BPlusCell<K>>(leafNode->cellsList, compare, createBPlusCell<K>(searchKey, NULL, NULL), searchType);
                if(foundLinkedNode){
                    return foundLinkedNode->key->key;
                }
            }
        }
        return NULL;
    }

    template<typename K>
    static std::shared_ptr<void> searchForValue( std::shared_ptr<BPlusTree<K>> tree, ComparatorFunction<BPlusCell<K>>  compare,std::shared_ptr<K> searchKey,SearchType searchType = SearchType::EqualsTo){
        auto leafNode = BB::searchForLeafNode(tree, compare, searchKey);
        if(leafNode){
            if(leafNode->cellsList){
                auto foundLinkedNode = LL::search<BPlusCell<K>>(leafNode->cellsList, compare, createBPlusCell<K>(searchKey, NULL, NULL), searchType);
                if(foundLinkedNode){
                    return foundLinkedNode->key->value;
                }
            }
        }
        return NULL;
    }

    template<typename K>
    static std::shared_ptr<std::vector<K>> searchForRangeWithPagination( std::shared_ptr<BPlusTree<K>> tree, ComparatorFunction<BPlusCell<K>>  compare,int offset=0,int limit=-1,std::shared_ptr<K> startKey=NULL,std::shared_ptr<K> endKey=NULL){
        auto startNode= !startKey? tree->left_most_node :  BB::searchForLeafNode(tree, compare, startKey);
        auto endNode= !endKey? tree->right_most_node: BB::searchForLeafNode(tree, compare, endKey);
        auto currentNode = startNode;

        int skip=0;
        int count=0;

        std::shared_ptr<std::vector<K>> result(new std::vector<K>());

        if(startNode!=endNode){
            while(currentNode!=endNode){
                auto sk = !startKey?NULL: createBPlusCell<K>(startKey, NULL, NULL);
                auto ek = !endKey? NULL: createBPlusCell<K>(endKey, NULL, NULL);
                if(currentNode && currentNode->cellsList){
                    std::shared_ptr<std::vector<std::shared_ptr<BPlusCell<K>>>>  st1 = LL::searchTillStream<BPlusCell<K>>(currentNode->cellsList, compare, sk, ek);
                    for(std::shared_ptr<BPlusCell<K>> &n1 : *st1){
                        if(skip>=offset){
                            if(count==limit){
                                break;
                            }
                            count++;
                            result->push_back(*n1->key);
                        }else{
                            skip++;
                        }
                    }
                    currentNode = currentNode->rightSibling.lock();;
                }
            }
        }
        if(currentNode && currentNode==endNode){
            auto sk = !startKey?NULL: createBPlusCell<K>(startKey, NULL, NULL);
            auto ek = !endKey? NULL: createBPlusCell<K>(endKey, NULL, NULL);
            if(currentNode && currentNode->cellsList){
                auto st1 = LL::searchTillStream<BPlusCell<K>>(currentNode->cellsList, compare, sk, ek);
                for(auto n1 : *st1){
                    if(skip>=offset) {
                        if (count == limit) {
                            break;
                        }
                        count++;
                        result->push_back(*n1->key);
                    }else{
                        skip++;
                    }
                }
            }
        }

        return result;
    }

    template<typename K>
    static std::shared_ptr<std::vector<std::shared_ptr<void>>> searchForRangeWithPaginationV( std::shared_ptr<BPlusTree<K>> tree, ComparatorFunction<BPlusCell<K>>  compare,int offset=0,int limit=-1,std::shared_ptr<K> startKey=NULL,std::shared_ptr<K> endKey=NULL){
        auto startNode= !startKey? tree->left_most_node :  BB::searchForLeafNode(tree, compare, startKey);
        auto endNode= !endKey? tree->right_most_node: BB::searchForLeafNode(tree, compare, endKey);
        auto currentNode = startNode;

        int skip=0;
        int count=0;

        std::shared_ptr<std::vector<std::shared_ptr<void>>> result(new std::vector<std::shared_ptr<void>>());

        if(startNode!=endNode){
            while(currentNode!=endNode){
                auto sk = !startKey?NULL: createBPlusCell<K>(startKey, NULL, NULL);
                auto ek = !endKey? NULL: createBPlusCell<K>(endKey, NULL, NULL);
                if(currentNode && currentNode->cellsList){
                    std::shared_ptr<std::vector<std::shared_ptr<BPlusCell<K>>>>  st1 = LL::searchTillStream<BPlusCell<K>>(currentNode->cellsList, compare, sk, ek);
                    for(std::shared_ptr<BPlusCell<K>> &n1 : *st1){
                        if(skip>=offset){
                            if(count==limit){
                                break;
                            }
                            count++;
                            result->push_back(n1->value);
                        }else{
                            skip++;
                        }
                    }
                    currentNode = currentNode->rightSibling.lock();
                }
            }
        }
        if(currentNode && currentNode==endNode){
            auto sk = !startKey?NULL: createBPlusCell<K>(startKey, NULL, NULL);
            auto ek = !endKey? NULL: createBPlusCell<K>(endKey, NULL, NULL);
            if(currentNode && currentNode->cellsList){
                auto st1 = LL::searchTillStream<BPlusCell<K>>(currentNode->cellsList, compare, sk, ek);
                for(auto n1 : *st1){
                    if(skip>=offset) {
                        if (count == limit) {
                            break;
                        }
                        count++;
                        result->push_back(n1->value);
                    }else{
                        skip++;
                    }
                }
            }
        }

        return result;
    }


    template<typename K>
    static std::shared_ptr<std::vector<K>> find( std::shared_ptr<BPlusTree<K>> tree,  ComparatorFunction<BPlusCell<K>>  compare ,  ComparatorFunction<BPlusCell<K>> queryComparator,std::shared_ptr<K> bookmark_key=NULL, bool yieldIndividualDuplicates=false){
        std::shared_ptr<std::vector<K>> result(new std::vector<K>());

        //will find leaf node with the same comparator as tree.
        std::shared_ptr<BPlusNode<K>> found_leaf_node;
        if(bookmark_key){
            found_leaf_node=BB::searchForLeafNode(tree, compare, bookmark_key, queryComparator);
        }else{
            if(tree->left_most_node && tree->left_most_node->cellsList && tree->left_most_node->cellsList->count>0){
                found_leaf_node=BB::searchForLeafNode(tree, compare, tree->left_most_node->cellsList->min->key->key, queryComparator);
            }
        }

        auto findingMatches = true;
        while(findingMatches){
            if(!found_leaf_node){
                findingMatches = false;
                break;
            }

            std::shared_ptr<BPlusCell<K>> startKey;//= bookmark_key? std::shared_ptr<BPlusCell<K>>(new BPlusCell(bookmark_key, NULL, found_leaf_node)) : found_leaf_node?->cellsList?->min?->key;
            if(bookmark_key){
                startKey = createBPlusCell<K>(bookmark_key, NULL, found_leaf_node);
            }else{
                if(found_leaf_node && found_leaf_node->cellsList && found_leaf_node->cellsList->min){
                    startKey=found_leaf_node->cellsList->min->key;
                }
            }

            if(found_leaf_node && found_leaf_node->cellsList){
                auto avlStr = LL::find<BPlusCell<K>>(found_leaf_node->cellsList, queryComparator);//LL::searchTillStream<BPlusCell<K>>(found_leaf_node->cellsList, compare, startKey, NULL, yieldIndividualDuplicates);

                for (auto &avlnode :  *avlStr){
                    if(bookmark_key){
                        bookmark_key=NULL;
                        continue;
                    }
                    auto searchKeyCurrentNode = queryComparator(createBPlusCell(avlnode->key),createBPlusCell(avlnode->key));
                    if(searchKeyCurrentNode==0){
                        result->push_back(*avlnode->key);
                    }
                }
                found_leaf_node = found_leaf_node->rightSibling;
            }
        }

        return result;
    }

    template<typename K>
    static std::shared_ptr<std::vector<std::shared_ptr<void>>> findV( std::shared_ptr<BPlusTree<K>> tree,  ComparatorFunction<BPlusCell<K>>  compare ,  ComparatorFunction<K> queryComparator,std::shared_ptr<K> bookmark_key=NULL, bool yieldIndividualDuplicates=false,uint limit=0){
        std::shared_ptr<std::vector<std::shared_ptr<void>>> result(new std::vector<std::shared_ptr<void>>());
        ComparatorFunction<BPlusCell<K>> ec = [queryComparator](std::shared_ptr<BPlusCell<K>> c1,std::shared_ptr<BPlusCell<K>> c2){
            return queryComparator(c1->key,c2->key);
        };

        //will find leaf node with the same comparator as tree.
        std::shared_ptr<BPlusNode<K>> found_leaf_node;
        if(bookmark_key){
            found_leaf_node=BB::searchForLeafNode(tree, compare, bookmark_key,ec);
        }else{
            if(tree->left_most_node && tree->left_most_node->cellsList && tree->left_most_node->cellsList->count>0){
                found_leaf_node=BB::searchForLeafNode(tree, compare, tree->left_most_node->cellsList->min->key->key, ec);
            }
        }

        auto findingMatches = true;
        while(findingMatches){
            if(!found_leaf_node){
                findingMatches = false;
                break;
            }

            std::shared_ptr<BPlusCell<K>> startKey;//= bookmark_key? std::shared_ptr<BPlusCell<K>>(new BPlusCell(bookmark_key, NULL, found_leaf_node)) : found_leaf_node?->cellsList?->min?->key;
            if(bookmark_key){
                startKey = createBPlusCell<K>(bookmark_key, NULL, found_leaf_node);
            }else{
                if(found_leaf_node && found_leaf_node->cellsList && found_leaf_node->cellsList->min){
                    startKey=found_leaf_node->cellsList->min->key;
                }
            }

            if(found_leaf_node && found_leaf_node->cellsList){
                auto avlStr = LL::find<BPlusCell<K>>(found_leaf_node->cellsList, ec);//LL::searchTillStream<BPlusCell<K>>(found_leaf_node->cellsList, compare, startKey, NULL, yieldIndividualDuplicates);

                for (auto &avlnode :  *avlStr){
                    if(bookmark_key){
                        bookmark_key=NULL;
                        continue;
                    }
                    auto searchKeyCurrentNode = queryComparator(avlnode->key,avlnode->key);//queryComparator(createBPlusCell(avlnode->key),createBPlusCell(avlnode->key));
                    if(searchKeyCurrentNode==0){
                        result->push_back(avlnode->value);
                        if(result->size()==limit){
                            break;
                        }
                    }
                }
                found_leaf_node = found_leaf_node->rightSibling.lock();
            }
        }

        return result;
    }

    //returns NULL if found no applicable leaf node
    template<typename K>
    static std::shared_ptr<K> insert( std::shared_ptr<BPlusTree<K>> tree, ComparatorFunction<BPlusCell<K>>  compare,std::shared_ptr<K> key,std::shared_ptr<void> value=NULL){
        if(!tree->root_node){
            tree->root_node=createBPlusNode(tree, true);
            tree->left_most_node=tree->root_node;
            tree->right_most_node=tree->root_node;
            tree->size++;
            auto j = LL::insert<BPlusCell<K>>(tree->root_node->cellsList, compare, createBPlusCell<K>(key,NULL,tree->root_node));
            j->key->value=value;
            return key;
        }

        auto leafNode = BB::searchForLeafNode(tree, compare, key);
        if(leafNode){
            auto insertedNode =  LL::insert<BPlusCell<K>>(leafNode->cellsList, compare, createBPlusCell<K>(key,NULL,leafNode));
            insertedNode->key->value=value;
            BB::balance<K>(tree, leafNode, compare);
            tree->size++;
            return key;
        }

        return NULL;
    }

    template<typename K>
    static std::shared_ptr<K> deleteKey(std::shared_ptr<BPlusTree<K>> tree, ComparatorFunction<BPlusCell<K>>  compare,std::shared_ptr<K> key) {
        if(!tree->root_node){
        tree->size=0;
        return NULL;
        }else{
        auto leafNode = BB::searchForLeafNode(tree, compare, key);
        auto deletedNode =  LL::deleteNode(leafNode->cellsList, compare, createBPlusCell<K>(key,NULL,NULL));
        if(deletedNode){
            tree->size=tree->size-(1+deletedNode->duplicate_count);
            BB::balance(tree, leafNode, compare);
            return deletedNode->key->key;
        }
        }
        return NULL;
    }

    template<typename K>
    static uint64_t getSize(std::shared_ptr<BPlusTree<K>> tree){
        return tree->size;
    }

    template<typename K>
    static std::shared_ptr<K> getMiddleKey(std::shared_ptr<BPlusTree<K>> tree){
        auto found_leaf_node = tree->left_most_node;
        auto hs = getSize(tree)/2;
        uint64_t c=0;
        
        if(!found_leaf_node){
            goto RETURN_NULL;
        }else{
            while(c<hs){
                auto t = c+found_leaf_node->cellsList->count;
                if(t<hs){
                    found_leaf_node=found_leaf_node->rightSibling.lock();
                }else{
                    return found_leaf_node->cellsList->min->key->key;
                }
                c=t;
            }
        }

        RETURN_NULL:
        return NULL;
    }
}
#endif // !BTREE

