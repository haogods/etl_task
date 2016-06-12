#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "python.h"

int test();
void quicksort(int a[], int low, int high)
{

    int middle;
    if(low >= high) return;
    middle = split(a, low, high);

    quicksort(a, low, middle - 1);
    quicksort(a, middle+1, high);
}


int split(int a[], int low, int high)
{
    int part_element = a[low];

    for(;;)
    {

        while(low < high && part_element <= a[high])
            high--;

        if (low >= high) break;

        a[low++] = a[high];

        while (low < high && part_element >= a[low])
            low++;

        if (low >= high) break;

        a[high--] = a[low];


    }

    a[high] = part_element;

    return high;

}

int fac(int n)
{

    if (n<2)
        return 1;

    return n * fac(n-1);

}


char *reverse(char *s)
{

    register char t, *p=s, *q=(s + (strlen(s) -1) );

    while (p < q)
    {

        t = *p;
        *p++ = *q;
        *q-- = t;

    }

    return s;
}

int productVerifyCode(char *s)
{

    if (strlen(s)<2) return -1;

    register char *a=s, *b=s+1, *end=s+strlen(s)-1;
    int odds=0, evens=0, tmp=0;
    while(a<=end)
    {

        tmp = *a - '0';
        if (tmp > 9 || tmp < 0) return 0;

        odds += tmp;

        tmp = 0;
        a += 2;

        if (b <= end)
        {

            tmp = *b - '0';
            if (tmp > 9 || tmp < 0) return 0;

            evens += tmp;


            tmp = 0;
            b += 2;

        }

    }

    return 9 - (odds * 3 + evens - 1) % 10 ;

}


static PyObject *
Extend1_fac(PyObject *self, PyObject* args){

    int res;
    int num;

    PyObject *retval;

    res = PyArg_ParseTuple(args, "i", &num);
    if(!res){

        return NULL;
    }

    res = fac(num);
    retval = (PyObject *)Py_BuildValue("i", res);
    return retval;

}

static PyObject *
Extend1_doppel(PyObject *self, PyObject *args)
{

    char *orig_str;
    char *dup_str;
    PyObject * retval;
    if(!PyArg_ParseTuple(args, "s", &orig_str)) return NULL;

    retval = (PyObject *)Py_BuildValue("ss", orig_str, dup_str=reverse(strdup(orig_str)));

    free(dup_str);
    return retval;

}

static PyObject *
Extend1_productVerifyCode(PyObject *self, PyObject *args)
{

    char *orig_str;
    PyObject *retval;
    if (!PyArg_ParseTuple(args, "s", &orig_str)) return NULL;

    retval = (PyObject *)Py_BuildValue("i", productVerifyCode(orig_str));

    return retval;

}

static PyObject *
Extend1_quicksort(PyObject *self, PyObject *args)
{

    int low, high, intp;

    PyObject *retval;

    if(!PyArg_ParseTuple(args, "iii", &intp, &low, &high)) return NULL;
    quicksort((int*)intp, low, high);
    retval = (PyObject *)Py_BuildValue("i", intp);

    return retval;


}



/*
     ** 我们想要做的最后一件是就是加上一个测试函数
     ** 我们把 main()函数改名为 test(),加个 Extest_test()函数把它包装起来，

*/
static PyObject *
Extend1_test(PyObject *self, PyObject *args){

    test();
    return (PyObject*)Py_BuildValue("");

}

/////////////////////////////////////////////////////////////

/*
    ** 现在,我们已经完成了两个包装函数。我们需要把它们列在某个地方,以便于 Python 解释器能 够导入并调用它们。这就是 ModuleMethods[]数组要做的事情。
    **
    ** 这个数组由多个数组组成。其中的每一个数组都包含了一个函数的信息。最后放一个 NULL 数组 表示列表的结束

*/

static PyMethodDef
Extend1Methods[] = {

    {"fac", Extend1_fac, METH_VARARGS},
    {"doppel", Extend1_doppel, METH_VARARGS},
    {"test", Extend1_test, METH_VARARGS},
    {"productVerifyCode", Extend1_productVerifyCode, METH_VARARGS},
    {"quicksort", Extend1_quicksort, METH_VARARGS},
    {NULL, NULL}

};


/*

    ** 所有工作的最后一部分就是模块的初始化函数。这部分代码在模块被导入的时候被解释器调用。
    **
    ** 在这段代码中,我们需要调用 Py_InitModule()函数,并把模块名和 ModuleMethods[]数组的名字传 递进去,以便于解释器能正确的调用我们模块中的函数。

*/

void initExtend1(){

    Py_InitModule("Extend1", Extend1Methods);

}




int test()
{

    char s[BUFSIZ];
    printf("4!== %d\n", fac(4));
    printf("8!== %d\n", fac(8));

    strcpy(s, "abcdefg");
    printf("reversing 'abcdefg', we get: '%s'\n", reverse(s));
    strcpy(s,"jiang");
    printf("reversing 'jianghao', we get: '%s'\n", reverse(s));

    strcpy(s, "01380015173");
    printf(" '01380015173', product verify code: %d\n", productVerifyCode(s));

    int a[] = {4,9,10,2, 3,1,20,11,23,16};

    printf("before sort: ");
    for(int i=0;i<10;i++){
        printf("%d ", a[i]);
    }

    printf("\nafter sort: ");
    quicksort(a, 0, 9);

    for(int i=0;i<10;i++){
        printf("%d ", a[i]);
    }

    return 0;

}