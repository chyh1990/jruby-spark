package com.sensetime.utils;

import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.ir.*;
import org.jruby.ir.instructions.Instr;
import org.jruby.ir.interpreter.InterpreterContext;
import org.jruby.ir.operands.*;
import org.jruby.ir.persistence.*;
import org.jruby.parser.StaticScope;
import org.jruby.parser.StaticScopeFactory;
import org.jruby.runtime.*;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;
import org.jruby.util.ByteList;
import org.jruby.util.KeyValuePair;

import java.io.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


class ProcIRWriterStream extends IRWriterStream {
    public ProcIRWriterStream(OutputStream stream) {
        super(stream);
    }

}

class ProcIRWriter {
    final static boolean PROCIR_PRINT = true;
    public static ArrayList<IRubyObject> persist(ThreadContext context,
                               IRWriterEncoder file, Block block) throws IOException {
        // ArrayList<IRScope> scopes;
        IRBlockBody irblock = (IRBlockBody)block.getBody();

        IRClosure closure = irblock.getScope();

        //IRubyObject self = block.getBinding().getSelf();
        System.out.println("X " + closure.getLexicalScopes());
        // IRScope s = closure;
        file.startEncoding(closure);

        // persistScopeInstrs(file, closure);
        persistScopeInstructions(file, closure);

        file.startEncodingScopeHeaders(closure);

        // write static scope
        ArrayList<IRScope> scopes = new ArrayList<>();
        IRScope parent = closure.getLexicalParent();
        while (parent != null) {
            if(PROCIR_PRINT) System.out.println("STATICSCOPE: " + parent.hashCode());
            scopes.add(parent);
            parent = parent.getLexicalParent();
        }
        file.encode(scopes.size());
        for (int i = scopes.size() - 1; i >= 0; i--) {
            persistScopeHeader(file, scopes.get(i), false, true);
        }

        persistScopeHeaders(file, closure, 0);

        file.endEncodingScopeHeaders(closure);

        // dynamic scopes
        DynamicScope dyn = block.getBinding().getDynamicScope();
        ArrayList<Integer> dynScopes = new ArrayList<>();
        while (dyn != null) {
            if(PROCIR_PRINT) System.out.println("DYNSCOPE: " + dyn.toString());
            // find scope
            int staticId = -1;
            for (int i = 0; i < scopes.size(); i++) {
                if (dyn.getStaticScope() == scopes.get(i).getStaticScope()) {
                    if (PROCIR_PRINT) System.out.println("FOUNDSTATIC: " + i + ", " + dyn.getStaticScope().toString());
                    staticId = i;
                    break;
                }
            }
            if (staticId < 0)
                throw context.getRuntime().newRuntimeError("Fail to find static scope of dynamicScope");
            // stored in revered order
            dynScopes.add(scopes.size() - staticId - 1);
            dyn = dyn.getParentScope();
        }
        // write in reversed order (parent -> child)
        file.encode(dynScopes.size());
        for (int i = dynScopes.size() - 1; i >= 0; i--)
            file.encode(dynScopes.get(i).intValue());

        if (PROCIR_PRINT) System.out.println("BINDING: " + block.getBinding().getFile() + ", "
                + block.getBinding().getLine() + ", frame " + block.getBinding().getFrame());
        // encode closure variables with indexes, just write the index mapping
        Map<LocalVariable, IRubyObject> closureOperands = analysisInstrs(context, block.getBinding(), closure);
        file.encode(closureOperands.size());
        ArrayList<IRubyObject> closureVars = new ArrayList();

        for (Map.Entry<LocalVariable, IRubyObject> entry : closureOperands.entrySet()) {
            //int id = closureVars.size();
            closureVars.add(entry.getValue());

            // name is not needed
            file.encode(entry.getKey().getName());
            file.encode(entry.getKey().getScopeDepth());
            file.encode(entry.getKey().getOffset());
            // file.encode(id);
        }

        file.endEncoding(closure);

        return closureVars;
    }

    private static Map<LocalVariable, IRubyObject> analysisInstrs(ThreadContext context, Binding binding, IRScope scope) {
        if (PROCIR_PRINT) System.out.println("Analysis instrs, self = " + binding.getSelf().toString()
            + ", dyn " + binding.getDynamicScope());
        Map<LocalVariable, IRubyObject> operands = new HashMap<>();

        // FIXME not always new DS, see commonYieldPath /org/jruby/runtime/MixedModeIRBlockBody.java
        InterpreterContext ic = scope.getInterpreterContext();
        if (!ic.pushNewDynScope() && !ic.reuseParentDynScope())
            throw context.getRuntime().newRuntimeError("unhandle dyn scope");
        DynamicScope dummyScope = DynamicScope.newDummyScope(scope.getStaticScope(), binding.getDynamicScope());

        for (Instr instr: scope.getInterpreterContext().getInstructions()) {
            for (Operand op : instr.getOperands()) {
                if (op instanceof LocalVariable) {
                    if (op instanceof ClosureLocalVariable) continue;
                    LocalVariable var = (LocalVariable)op;

                    if (PROCIR_PRINT) System.out.println("doing: " + var.toString());
                    if (operands.get(op) == null) {
                        IRubyObject obj = (IRubyObject)var.retrieve(context, binding.getSelf(), scope.getStaticScope(), dummyScope, null);
                        operands.put(var, obj);
                        if (PROCIR_PRINT) System.out.println(var.getName() + " : " + obj.getClass().getName() + ", " + obj.toString());
                    }
                }
            }
        }
        return operands;
    }

    private static void persistScopeHeaders(IRWriterEncoder file, IRScope parent, int depth) {
        persistScopeHeader(file, parent, true, depth == 0);

        for (IRScope scope: parent.getLexicalScopes()) {
            persistScopeHeaders(file, scope, depth+1);
        }
    }

    private static void persistScopeInstrs(IRWriterEncoder file, IRScope scope) {
        if (PROCIR_PRINT) System.out.println("Writing instr " + scope);
        file.startEncodingScopeInstrs(scope);

        for (Instr instr: scope.getInterpreterContext().getInstructions()) {
            file.encode(instr);
        }

        file.endEncodingScopeInstrs(scope);
    }


    private static void persistScopeInstructions(IRWriterEncoder file, IRScope parent) {
        persistScopeInstrs(file, parent);

        for (IRScope scope: parent.getLexicalScopes()) {
            persistScopeInstructions(file, scope);
        }
    }

    // script body: {type,name,linenumber,{static_scope},instrs_offset}
    // closure scopes: {type,name,linenumber,lexical_parent_name, lexical_parent_line,is_for,arity,arg_type,{static_scope},instrs_offset}
    // other scopes: {type,name,linenumber,lexical_parent_name, lexical_parent_line,{static_scope}, instrs_offset}
    // for non-for scopes is_for,arity, and arg_type will be 0.
    private static void persistScopeHeader(IRWriterEncoder file, IRScope scope,
                                           boolean hasInstr, boolean outmost) {
        if (PROCIR_PRINT) System.out.println("Writing Scope Header");
        file.startEncodingScopeHeader(scope);
        if (PROCIR_PRINT) System.out.println("IRScopeType = " + scope.getScopeType());
        file.encode(scope.getScopeType()); // type is enum of kind of scope
        if (PROCIR_PRINT) System.out.println("NAME = " + scope.getName());
        file.encode(scope.getName());
        if (PROCIR_PRINT) System.out.println("Line # = " + scope.getLineNumber());
        file.encode(scope.getLineNumber());
        if (PROCIR_PRINT) System.out.println("# of temp vars = " + scope.getTemporaryVariablesCount());
        file.encode(scope.getTemporaryVariablesCount());

        persistScopeLabelIndices(scope, file);

        if (!(scope instanceof IRScriptBody) && !outmost)
            file.encode(scope.getLexicalParent());

        if (scope instanceof IRClosure) {
            IRClosure closure = (IRClosure) scope;

            file.encode(closure.getSignature().encode());
        }

        persistStaticScope(file, scope.getStaticScope());
        persistLocalVariables(scope, file);
        if (hasInstr)
            file.endEncodingScopeHeader(scope);
        else
            file.encode(0);
    }

    // FIXME: I hacked around our lvar types for now but this hsould be done in a less ad-hoc fashion.
    private static void persistLocalVariables(IRScope scope, IRWriterEncoder file) {
        Map<String, LocalVariable> localVariables = scope.getLocalVariables();
        file.encode(localVariables.size());
        for (String name: localVariables.keySet()) {
            if (PROCIR_PRINT) System.out.println("LOCAL VAR: " + name);
            file.encode(name);
            file.encode(localVariables.get(name).getOffset()); // No need to write depth..it is zero.
        }
    }

    private static void persistScopeLabelIndices(IRScope scope, IRWriterEncoder file) {
        Map<String,Integer> labelIndices = scope.getVarIndices();
        if (PROCIR_PRINT) System.out.println("LABEL_SIZE: " + labelIndices.size());
        file.encode(labelIndices.size());
        for (String key : labelIndices.keySet()) {
            if (PROCIR_PRINT) System.out.println("LABEL: " + key);
            file.encode(key);
            file.encode(labelIndices.get(key).intValue());
            if (PROCIR_PRINT) System.out.println("LABEL(num): " + labelIndices.get(key).intValue());
        }
        if (PROCIR_PRINT) System.out.println("DONE LABELS: " + labelIndices.size());
    }

    // {type,[variables],signature}
    private static void persistStaticScope(IRWriterEncoder file, StaticScope staticScope) {
        file.encode(staticScope.getType());
        file.encode(staticScope.getVariables());
        file.encode(staticScope.getSignature());
    }
}

class ProcIRReader extends IRReader {
    final static boolean PROCIR_PRINT = true;

    public static Block loadToScope(Ruby runtime, IRReaderDecoder file, IRubyObject[] vars) throws IOException {
        int version = file.decodeIntRaw();

        if (version != VERSION) {
            throw new IOException("Trying to read incompatable persistence format (version found: " +
                    version + ", version expected: " + VERSION);
        }
        int headersOffset = file.decodeIntRaw();
        if (PROCIR_PRINT) System.out.println("header_offset = " + headersOffset);
        int poolOffset = file.decodeIntRaw();
        if (PROCIR_PRINT) System.out.println("pool_offset = " + headersOffset);

        file.seek(headersOffset);

        // decode scopes
        int scopesToRead  = file.decodeInt();
        int outerScopeToRead = file.decodeInt();

        if (PROCIR_PRINT) System.out.println("outer scopes to read = " + outerScopeToRead);
        // KeyValuePair<String, IRScopeType> outerScopes[] = new KeyValuePair[outerScopeToRead];
        IRScope outerScopes[] = new IRScope[outerScopeToRead];

        IRScope rootScope = decodeScopeHeader(runtime.getIRManager(), file, null, false).getKey();
        rootScope.setFileName("<remote>:"+rootScope.getFileName());

        RubyModule current = runtime.getObject();
        rootScope.getStaticScope().setModule(current);

        outerScopes[0] = rootScope;

        for (int i = 1; i < outerScopeToRead; i++) {
            // IRScopeType type = file.decodeIRScopeType();
            // String name = file.decodeString();
            IRScope scope = decodeScopeHeader(runtime.getIRManager(), file, outerScopes[i-1], true).getKey();
            if (scope instanceof IRModuleBody) {
                IRubyObject obj = current.getConstant(scope.getName());
                if (obj == null) {
                    throw runtime.newNameError("scope name not found", scope.getName());
                }
                if (!(obj instanceof RubyModule)) {
                    throw runtime.newRuntimeError(scope.getName() + " is no a ruby module");
                }
                current = (RubyModule)obj;
                scope.getStaticScope().setModule(current);
            } else {
                scope.getStaticScope().determineModule();
            }
            outerScopes[i] = scope;
            if (PROCIR_PRINT) System.out.println("outer scopes to read " + i + " :" + outerScopes[i].toString()
                    + ", mod:" + scope.getStaticScope().getModule().toString());
        }
        /*
        if (outerScopes[0].getScopeType() == IRScopeType.SCRIPT_BODY) {
            IRScriptBody s = (IRScriptBody)outerScopes[0];
            s.setFileName("<remote>:"+s.getFileName());
        }*/
        // resolve scopes

       // for (KeyValuePair<String, IRScopeType> pair: outerScopes) {
            //for (rootStaticScope.getModule()

        //}

        if (PROCIR_PRINT) System.out.println("scopes to read = " + scopesToRead);

        KeyValuePair<IRScope, Integer>[] scopes = new KeyValuePair[scopesToRead];
        for (int i = 0; i < scopesToRead; i++) {
            scopes[i] = decodeScopeHeader(runtime.getIRManager(), file,
                    outerScopes[outerScopeToRead-1], i == 0);
            file.addScope(scopes[i].getKey());
        }

        // read dynamic scope
        int dynScopeToRead = file.decodeInt();
        if (PROCIR_PRINT) System.out.println("dyn scopes to read = " + dynScopeToRead);
        DynamicScope currentScope = null;
        for (int i = 0; i < dynScopeToRead; i++) {
            int idx = file.decodeInt();
            if (PROCIR_PRINT) System.out.println("dyn scopes " + idx + ": " + outerScopes[idx]);
            // reconstruct dynamic scopes
            currentScope = DynamicScope.newDynamicScope(outerScopes[idx].getStaticScope(), currentScope);
        }

        // construct dynamic scopes
        int closureVarsToRead = file.decodeInt();
        if (PROCIR_PRINT) System.out.println("scopes var to read = " + closureVarsToRead);
        int selfIdx = -1;
        for (int i = 0; i < closureVarsToRead; i++) {
            String name = file.decodeString();
            // FIXME detect self with flags
            int depth = file.decodeInt();
            int offset = file.decodeInt();
            if (PROCIR_PRINT) System.out.println("scopes var " + i + ": " + name + "(" + depth + ":" + offset + ")");

            // setup
            if (name.equals("%self")) {
                selfIdx = i;
            } else {
                if (depth <= 0) {
                    throw new IllegalArgumentException("offset too small");
                } else {
                    // FIXME lambda in an extra depth
                    currentScope.setValue(offset, vars[i], depth - 1);
                }
            }
        }

        // FIXME: if self is not used, set to null?
        IRubyObject self = runtime.getTopSelf();
        if (selfIdx >= 0)
            self = vars[selfIdx];
        if (PROCIR_PRINT) System.out.println("Self: " + self.toString());

        // Lifecycle woes.  All IRScopes need to exist before we can decodeInstrs.
        for (KeyValuePair<IRScope, Integer> pair: scopes) {
            if (PROCIR_PRINT) System.out.println("instr scope: " + pair.getKey() +" off " + pair.getValue());
            if (PROCIR_PRINT) System.out.println("instr parent scope: " + pair.getKey().getLexicalParent());
            IRScope scope = pair.getKey();
            int instructionsOffset = pair.getValue();

            scope.allocateInterpreterContext(file.decodeInstructionsAt(scope, instructionsOffset));
        }

        IRClosure closure = (IRClosure)scopes[0].getKey(); // closure scope;
        Binding binding = new Binding(self, new Frame(), Visibility.PUBLIC, currentScope, "<noname>", "<noname>", 0);
        Block block = new Block(closure.getBlockBody(), binding);
        return block;
    }

    private static KeyValuePair<IRScope, Integer> decodeScopeHeader(IRManager manager,
                                                                    IRReaderDecoder decoder,
                                                                    IRScope parentScope,
                                                                    boolean outmost_closure) {
        if (PROCIR_PRINT) System.out.println("DECODING SCOPE HEADER");
        IRScopeType type = decoder.decodeIRScopeType();
        if (PROCIR_PRINT) System.out.println("IRScopeType = " + type);
        String name = decoder.decodeString();
        if (PROCIR_PRINT) System.out.println("NAME = " + name);
        int line = decoder.decodeInt();
        if (PROCIR_PRINT) System.out.println("LINE = " + line);
        int tempVarsCount = decoder.decodeInt();
        if (PROCIR_PRINT) System.out.println("# of Temp Vars = " + tempVarsCount);
        Map<String, Integer> indices = decodeScopeLabelIndices(decoder);

        IRScope parent = parentScope;
        // type != IRScopeType.SCRIPT_BODY ? decoder.decodeScope() : null;
        if (type != IRScopeType.SCRIPT_BODY && !outmost_closure) {
            parent = decoder.decodeScope();
            if (PROCIR_PRINT) System.out.println("# outscope = " + parent);
        }
        // IRScope parent = null;
        Signature signature;

        if (type == IRScopeType.CLOSURE || type == IRScopeType.FOR) {
            signature = Signature.decode(decoder.decodeLong());
        } else {
            signature = Signature.OPTIONAL;
        }
        // StaticScope parentScope = parent == null ? null : parent.getStaticScope();

        // FIXME: It seems wrong we have static scope + local vars both being persisted.  They must have the same values
        // and offsets?
        StaticScope staticScope = decodeStaticScope(decoder, parent == null ? null : parent.getStaticScope());
        IRScope scope = createScope(manager, type, name, line, parentScope, signature, staticScope);

        scope.setTemporaryVariableCount(tempVarsCount);
        // FIXME: Replace since we are defining this...perhaps even make a persistence constructor
        scope.setLabelIndices(indices);

        // FIXME: This is odd, but ClosureLocalVariable wants it's defining closure...feels wrong.
        // But because of this we have to push decoding lvars to the end of the scope info.
        scope.setLocalVariables(decodeScopeLocalVariables(decoder, scope));

        int instructionsOffset = decoder.decodeInt();

        return new KeyValuePair<>(scope, instructionsOffset);
    }

    private static Map<String, LocalVariable> decodeScopeLocalVariables(IRReaderDecoder decoder, IRScope scope) {
        int size = decoder.decodeInt();
        Map<String, LocalVariable> localVariables = new HashMap(size);
        for (int i = 0; i < size; i++) {
            String name = decoder.decodeString();
            int offset = decoder.decodeInt();

            localVariables.put(name, scope instanceof IRClosure ?
                    // SSS FIXME: do we need to read back locallyDefined boolean?
                    new ClosureLocalVariable(name, 0, offset) : new LocalVariable(name, 0, offset));
        }

        return localVariables;
    }

    private static Map<String, Integer> decodeScopeLabelIndices(IRReaderDecoder decoder) {
        int labelIndicesSize = decoder.decodeInt();
        Map<String, Integer> indices = new HashMap<String, Integer>(labelIndicesSize);
        for (int i = 0; i < labelIndicesSize; i++) {
            indices.put(decoder.decodeString(), decoder.decodeInt());
        }
        return indices;
    }

    private static StaticScope decodeStaticScope(IRReaderDecoder decoder, StaticScope parentScope) {
        StaticScope scope = StaticScopeFactory.newStaticScope(parentScope, decoder.decodeStaticScopeType(), decoder.decodeStringArray());

        scope.setSignature(decoder.decodeSignature());

        return scope;
    }
}

/**
 * Created by chenyh on 3/26/16.
 */
public class ProcToBytesService implements BasicLibraryService {
    static Field fBlock;

    protected static final ObjectAllocator NEW_PROC_ALLOCATOR = new ObjectAllocator() {
        public IRubyObject allocate(Ruby runtime, RubyClass klass) {
            Block dummy = new Block(new NullBlockBody(), new Binding(new Frame(), null, "", "", 0));
            return RubyProc.newProc(runtime, dummy, Block.Type.LAMBDA);
        }
    };

    @Override
    public boolean basicLoad(Ruby runtime) throws IOException {
        runtime.getClass("Proc").defineAnnotatedMethods(ProcToBytes.class);

        // FIXME hack, JRuby do not implement _load, have to set block with reflection
        runtime.getClass("Proc").setAllocator(NEW_PROC_ALLOCATOR);
        try {
            fBlock = RubyProc.class.getDeclaredField("block");
            fBlock.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        return true;
    }


    @JRubyClass(name = "ProcToBytes")
    public static class ProcToBytes {
        @JRubyMethod
        public static IRubyObject marshal_load(ThreadContext context, IRubyObject self, IRubyObject _args) {
            // System.out.println("XX " + _args);
            if  (!(_args instanceof RubyArray))
                throw context.getRuntime().newArgumentError("must be array");

            IRubyObject [] args = ((RubyArray)_args).toJavaArray();
            if (args.length != 2)
                throw context.getRuntime().newArgumentError(args.length, 2);

            // FIXME hack
            RubyProc proc = (RubyProc)_from_bytes(context, null, args[0], args[1]);
            try {
                fBlock.set(self, proc.getBlock());
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            return null;
        }

        @JRubyMethod(meta = true)
        public static IRubyObject _from_bytes(ThreadContext context, IRubyObject recv, IRubyObject data, IRubyObject _vars) {
            // RubyModule object = context.getRuntime().getObject();

            //throw context.getRuntime().newRuntimeError("unimpl");
            ByteList bytes = data.convertToString().getByteList();

            if  (!(_vars instanceof RubyArray))
                throw context.getRuntime().newArgumentError("must be array");
            RubyArray vars = (RubyArray)_vars;

            ByteArrayInputStream is = new ByteArrayInputStream(bytes.getUnsafeBytes());
            System.out.println("IRScope: " + context.getCurrentStaticScope().getIRScope());

            // ObjectInputStream input = null;
            try {
                IRReaderStream input = new IRReaderStream(context.getRuntime().getIRManager(), is, null);

                Block block = ProcIRReader.loadToScope(context.getRuntime(), input, vars.toJavaArray());
                // System.out.println("XX " + scope.toString() + " " + scope.toStringInstrs());
                //IRClosure closure = (IRClosure)scope;
                //closure.getBlockBody();
                // Block block = new Block(closure.getBlockBody(), context.currentBinding());
                return RubyProc.newProc(context.getRuntime(), block, Block.Type.LAMBDA);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @JRubyMethod
        public static IRubyObject marshal_dump(ThreadContext context, IRubyObject self) {
            Ruby ruby = context.getRuntime();
            RubyProc proc = (RubyProc) self;
            if (!proc.lambda_p(context).isTrue())
                throw context.getRuntime().newArgumentError("only lambda can be dump");

           // System.err.println("XX " + proc);

            Block block = proc.getBlock();


            System.err.println("DYN Scope: " + block.getBinding().getDynamicScope());
            IRBlockBody irblock = (IRBlockBody)block.getBody();

            IRClosure closure = irblock.getScope();
            System.err.println("INSTR: " + closure.toStringInstrs());
            System.err.println("SS Scope: " + closure.getStaticScope().toString());


            try {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    ArrayList<IRubyObject> vars = ProcIRWriter.persist(context, new ProcIRWriterStream(baos), block);

                    RubyArray ret = ruby.newArray(2);
                    ret.set(0, ruby.newString(new ByteList(baos.toByteArray())));
                    ret.set(1, ruby.newArray(vars));

                    return ret;
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
