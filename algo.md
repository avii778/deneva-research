# Pro-Life Transactions
# Pro-Life Transactions

## Transaction Type

A transaction consists of:

- `pid`: Identifier of a reusable virtual process name. Its node component is
  the originating node. Its worker component encodes `(name, worker)` as
  `name * worker_count + worker`. A physical process may use multiple names
  concurrently, but each name runs at most one transaction at a time.
- `tid`: A tuple `(time, attempt)` where:
  - `time` is a globally unique identifier for this transaction.
  - `attempt` is the current attempt number of this transaction.

### Transaction Priority

Define `tida <p tidb` to mean `tida` has higher priority than `tidb`.

Formally:

```
tida <p tidb iff

(tida.time < tidb.time)
OR
(tida.time = tidb.time AND tida.attempt > tidb.attempt)
```

### Additional Fields

- `state`: The state of this transaction.
- `history`: A sequence of tuples `(obj, op, r)` where:
  - `obj` is an object.
  - `op` is an operation applied to `obj`.
  - `r` is the response.

Define:

```
history[obj]
```

as the subsequence of `history` whose first field is `obj`.

- `run`: The run function for this transaction.

---

# Object State

For an object `O` of type `T`:

- `S`: State of the underlying object `O`, initially the initial state of type `T`.
- `A`: Identifier of the process that most recently modified this object, initially `None`.
- `P`: Mapping from process identifiers and `None` to `(transaction, status)`.

Virtual process names are returned to their originating process after a
transaction terminates. If a name is reassigned, the new transaction's
`tid.time` must be strictly greater than the previous assignment's time.

Status values:

- `Executing`
- `Prepared`
- `Committed`
- `Aborted`

Initially:

```
P[x] = (0, Aborted)
```

for all process identifiers, except:

```
P[None] = (∞, Aborted)
```

- `InLine`: The highest-priority transaction and next operation helping a prepared transaction.

Initially:

```
InLine = None
```

If `InLine = None`, then:

```
InLine.tid = (∞, ∞)
```

- `PHeap`: An indexed priority heap containing one node for every transaction
  that has touched this object and has not committed. Nodes are ordered by
  `<p`. The node is stored with the transaction's reusable `P` slot, so a
  higher attempt updates and reheapifies the existing node rather than adding
  a duplicate.

`config.h` exposes the compile-time toggle `life_fairness`. When true,
`Row_life` uses `PHeap` and releases committed descriptors immediately. When
false, it uses the original single-`A` helping policy and descriptor lifetime.

---

# Algorithm 1: Container Object Specification

```text
1  Execute(tx, op)
2      (ctx, cstatus) := P[A]
3      (ltx, lstatus) := P[tx.pid]

4      if
           cstatus = Prepared
        OR (PHeap != empty AND PHeap.top.tid <p tx.tid)
        OR tx.tid < ltx.tid
        OR (tx.tid = ltx.tid
            AND (lstatus = Aborted
                 OR |tx.history| < |ltx.history|))
       then

5          if cstatus = Prepared then

6              if tx.tid ≤p InLine.tid then
                   InLine := (tx, op)

7              return (Finalize, ctx)

8          else if PHeap != empty
                    AND PHeap.top.tid <p tx.tid then
               return (Help, PHeap.top)

9          else if tx.tid.time < ltx.tid.time then
               return (Committed, *)

10         else if tx.tid < ltx.tid OR lstatus = Aborted then
               return (Retry, ltx.tid.attempt)

11         else if |tx.history| < |ltx.history| then
               return (
                   Success,
                   ltx.history[O][|tx.history[O]| + 1].response
               )

12     else if
           cstatus ≠ Prepared
        AND (PHeap = empty OR tx.tid ≤p PHeap.top.tid)
        AND ltx.tid ≤ tx.tid
        AND (
             tx.tid ≠ ltx.tid
             OR (
                 lstatus ≠ Aborted
                 AND |ltx.history| ≤ |tx.history|
             )
        )
       then

13         PHeap.insert-or-update(tx)

14         P[A].status := Aborted

15         ((-, op1, -), ..., (-, opn, -))
              := tx.history[O]

16         s := S

17         foreach i ∈ [1..n] do
               (s, -) := applyT(opi, s)

18         (-, response) := applyT(op, s)

19         A := tx.pid

20         P[tx.pid] := (tx, Executing)

21         tx.history.append((O, op, response))

22         return (Success, response)

22 Prepare(tx)

23     (ltx, lstatus) := P[tx.pid]

24     if tx.tid.time < ltx.tid.time
           OR lstatus = Committed
       then
           return (Success, *)

25     else if
           tx.tid.attempt < ltx.tid.attempt
           OR lstatus = Aborted
       then
           return (Retry, ltx.tid.attempt)

26     P[tx.pid] := (tx, Prepared)

27     return (Success, *)

28 Commit(tx)

29     (ltx, lstatus) := P[tx.pid]

30     if tx.tid.time < ltx.tid.time
           OR lstatus = Committed
       then return

31     ((-, op1, -), ..., (-, opn, -))
           := tx.history[O]

32     foreach i ∈ [1..n] do
           (S, -) := applyT(opi, S)

33     P[tx.pid].status := Committed

34     PHeap.remove(tx)

35     Help(tx)

36 Rollback(tx)

36     (ltx, lstatus) := P[tx.pid]

37     if tx.tid = ltx.tid
           AND lstatus ≠ Aborted
       then

38         P[tx.pid].status := Aborted

39     Help(tx)

40 routine Help(tx)

41     if A = tx.pid then

42         A := None

43         if InLine.tx ≠ None then

44             Execute(InLine.tx, InLine.op)

45             InLine := (None, None)
```

---

# Algorithm 2: Nondeterministic Abort-Free Transactions

```text
1  procedure DoTransaction(run)
2      tx := (
           pid,
           (unique time, 1),
           Init,
           [(-, -, Init)],
           run
       )

3      TryToDoTransaction(tx)

4      ((obj0,o0,r0),...,(objn,on,rn))
           := tx.history

5      return
           (obj1,o1,r1),...,(objn,on,rn)

6  procedure TryToDoTransaction(tx)

7      txns := [tx]

8      while |txns| > 0 do

9          ctx := txns.tail()

10         (-,-,response) := ctx.history.tail()

11         (ctx.state,(obj,op))
               := ctx.run(ctx.state,response)

12         if ctx.state = End then

13             if FinalizeTransaction(ctx)
                   then txns.pop()

14         else if ctx.state ≠ End then

15             (status,response)
                   := obj.Execute(ctx,op)

16             if status = Finalize then

17                 FinalizeTransaction(response)

18                 if response.tid.time
                        < ctx.tid.time
                    then txns.push(response)

20             else if status = Help then

21                 foreach t ∈ txns do

22                     if t.tid.time
                           = response.tid.time
                        then txns.delete(t)

24                 txns.push(response)

25             else if status = Committed then
                   txns.pop()

27             else if status = Retry then
                   ResetTransaction(ctx,response)

29             else if status = Success then
                   ctx.history.append(
                       (obj,op,response)
                   )

31 procedure FinalizeTransaction(tx)

32     foreach obj ∈ tx.history.keys() do

33         (status,response)
                := obj.Prepare(tx)

34         if status = Retry then

35             foreach obj ∈ tx.history.keys() do
                   obj.Rollback(tx)

37             ResetTransaction(tx,response)

38             return False

39     foreach obj ∈ tx.history.keys() do
           obj.Commit(tx)

41     return True

42 procedure ResetTransaction(tx,a)

43     tx.tid.attempt := a + 1

44     tx.state := Init

45     tx.history := [(-,-,Init)]
```

---

# Algorithm 3: Nondeterministic Abort-Free Transactions (Batched)

```text
1  procedure DoTransaction(run)

2      tx := (
           pid,
           (unique time,1),
           Init,
           [[(-,-,Init)]],
           run
       )

3      TryToDoTransaction(tx)

4      ((obj0,o0,r0),...,(objn,on,rn))
           := tx.history

5      return
           (obj1,o1,r1),...,(objn,on,rn)

6  procedure TryToDoTransaction(tx)

7      txns := [tx]

8      while |txns| > 0 do

9          ctx := txns.tail()

10         (-,-,responses)
                := ctx.history.tail()

11         (ctx.state,ops)
                := ctx.run(ctx.state,response)

12         if ctx.state = End then

13             if FinalizeTransaction(ctx)
                    then txns.pop()

14         else if ctx.state ≠ End then

15             responses := []

16             foreach (obj,op) ∈ ops do

17                 (status,response)
                        := obj.Execute(ctx,op)

18                 if status = Finalize then

19                     FinalizeTransaction(response)

20                     if response.tid.time
                            < ctx.tid.time
                        then txns.push(response)

22                 else if status = Help then

23                     foreach t ∈ txns do

24                         if t.tid.time
                                = response.tid.time
                            then txns.delete(t)

26                     txns.push(response)

27                 else if status = Committed then
                       txns.pop()

29                 else if status = Retry then
                       ResetTransaction(ctx,response)

31                 else if status = Success then
                       responses.append(
                           (obj,op,response)
                       )

33                 if status ≠ Success then
                       continue loop on line 8

34             ctx.history.append(responses)

35 procedure FinalizeTransaction(tx)

36     foreach obj ∈ tx.history.keys() do

37         (status,response)
                := obj.Prepare(tx)

38         if status = Retry then

39             foreach obj ∈ tx.history.keys() do
                   obj.Rollback(tx)

41             ResetTransaction(tx,response)

42             return False

43     foreach obj ∈ tx.history.keys() do
           obj.Commit(tx)

45     return True

46 procedure ResetTransaction(tx,a)

47     tx.tid.attempt := a + 1

48     tx.state := Init

49     tx.history := [(-,-,Init)]
```
