#include"./src/src/core/desto.h"
#include"../indexInterface.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class DESTOOLInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
    void init(Param *param = nullptr) {}

    void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

    bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

    bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool remove(KEY_TYPE key, Param *param = nullptr);

    size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                Param *param = nullptr);

    long long memory_consumption() { return destool.total_size(); }

private:
    destool::LIPP <KEY_TYPE, PAYLOAD_TYPE> destool;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void DESTOOLInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                      Param *param) {
    destool.bulk_load(key_value, static_cast<int>(num));
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool DESTOOLInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
    return destool.at(key, &val);
    // return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool DESTOOLInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    destool.insert(key, value);
    return true;
    // return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool DESTOOLInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    // return destool.update(key, value);
    return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool DESTOOLInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
    // return destool.remove(key);
    return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
size_t DESTOOLInterface<KEY_TYPE, PAYLOAD_TYPE>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                   std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                                                   Param *param) {
    auto scan_size = destool.range_scan_by_size(key_low_bound, static_cast<uint32_t>(key_num), result);
    return scan_size;
}