#include <cstdlib>
#include <iostream>
#include <string>

using namespace std;

const char alphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 
    'q', 'r', 's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

// 生成随机字符串
string genRandomString(int len) {
    string result;
    int alphabet_len = sizeof(alphabet);
    for (int i = 0; i < len; ++i) {
        result.append(1, alphabet[rand() % alphabet_len]);
    }

    return result;
}

void genRandomDataSet(int nums) {
    srand((unsigned int)(time(NULL)));
    for (int i = 0; i < nums; ++i) {
        cout << genRandomString(16) << "," << genRandomString(32) << endl;
    }
}


int main(int argc, char *argv[]) {
    // if (argc < 2) {
    //     cout << "missing param nums!" << endl;
    //     return 0;
    // }
    // int nums = std::stoi(string(argv[1]));
    // genRandomDataSet(nums);

    for (int i = 0; i < 10; ++i) {
        cout << genRandomString(4) << endl;
    }

    return 0;
}