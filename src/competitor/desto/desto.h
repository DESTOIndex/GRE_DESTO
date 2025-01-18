#include"./src/src/core/desto.h"
#include"../indexInterface.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class DESTOInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
    void init(Param *param = nullptr) {}

    void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

    bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

    bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool remove(KEY_TYPE key, Param *param = nullptr);

    size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                Param *param = nullptr);

    long long memory_consumption() { return desto.total_size(); }

private:
    desto::LIPP <KEY_TYPE, PAYLOAD_TYPE> desto;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void DESTOInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                      Param *param) {
    // std::cout << " sizeof(KEY_TYPE) " << sizeof(KEY_TYPE) <<  " sizeof(PAYLOAD_TYPE) " << sizeof(PAYLOAD_TYPE);
    desto.bulk_load(key_value, static_cast<int>(num));
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool DESTOInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
    bool exist;
    val = desto.at(key, false, exist);
    return exist;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool DESTOInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return desto.insert(key, value);

}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool DESTOInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return desto.update(key, value);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool DESTOInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
    return desto.remove(key);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
size_t DESTOInterface<KEY_TYPE, PAYLOAD_TYPE>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                   std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                                                   Param *param) {
    auto iter = desto.lower_bound(key_low_bound);
    int scan_size = 0;
    for (scan_size = 0; scan_size < key_num && !iter.is_end(); scan_size++) {
        result[scan_size] = {(*iter).first, (*iter).second};
        iter++;
    }
    return scan_size;    
}