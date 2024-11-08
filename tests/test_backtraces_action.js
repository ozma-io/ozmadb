async function inside1() {
    console.WriteLine("\nInside inside1-1");
    console.WriteLine(new Error().stack);
    await engine.Yield();
    console.WriteLine("\nInside inside1-2");
    console.WriteLine(new Error().stack);
    // engine.ThrowHostException("inside1s");
}

async function inside2() {
    await engine.YieldAndRun(inside1);
}

async function nested() {
    console.WriteLine("\nStack1");
    console.WriteLine(new Error().stack);
    await inside1();
    console.WriteLine("\nStack2");
    await engine.Yield();
    console.WriteLine("\nend Stack2");
    console.WriteLine(new Error().stack);
    await inside1();
    console.WriteLine("\nYield 1");
    await engine.YieldAndRun(inside1);
    console.WriteLine("\nYield 2");
    await engine.YieldAndRun(inside2);
}

export default async function t(args) {
    await nested();
}
