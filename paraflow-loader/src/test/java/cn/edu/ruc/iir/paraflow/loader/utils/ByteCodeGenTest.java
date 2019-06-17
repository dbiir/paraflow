package cn.edu.ruc.iir.paraflow.loader.utils;

import org.junit.Test;
import org.objectweb.asm.ClassWriter;

import static org.objectweb.asm.Opcodes.ACC_ABSTRACT;
import static org.objectweb.asm.Opcodes.ACC_FINAL;
import static org.objectweb.asm.Opcodes.ACC_INTERFACE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.V1_6;

/**
 * paraflow
 *
 * @author guodong
 */
public class ByteCodeGenTest
{
    @Test
    public void genClass()
    {
        ClassWriter classWriter = new ClassWriter(0);
        classWriter.visit(V1_6, ACC_PUBLIC + ACC_ABSTRACT + ACC_INTERFACE,
                "cn/edu/ruc/iir/paraflow/loader/consumer/utils/Comparable",
                null,
                "java/lang/Object",
                null);
        classWriter.visitField(ACC_PUBLIC + ACC_FINAL + ACC_STATIC,
                "LESS", "I", null, -1)
                .visitEnd();
        classWriter.visitField(ACC_PUBLIC + ACC_FINAL + ACC_STATIC,
                "EQUAL", "I", null, 0)
                .visitEnd();
        classWriter.visitField(ACC_PUBLIC + ACC_FINAL + ACC_STATIC,
                "GREATER", "I", null, 1)
                .visitEnd();
        classWriter.visitMethod(ACC_PUBLIC + ACC_ABSTRACT,
                "compareTo", "(Ljava/lang/Object;)I", null, null)
                .visitEnd();
        classWriter.visitEnd();
        byte[] bytes = classWriter.toByteArray();

        MyClassLoader myClassLoader = new MyClassLoader();
        Class clazz = myClassLoader.defineClass("cn.edu.ruc.iir.paraflow.loader.consumer.utils.Comparable", bytes);
        try {
            System.out.println(clazz.getField("LESS").getName());
        }
        catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void printClass()
    {
        genClass();
    }

    class MyClassLoader extends ClassLoader
    {
        Class defineClass(String name, byte[] b)
        {
            return defineClass(name, b, 0, b.length);
        }
    }
}
