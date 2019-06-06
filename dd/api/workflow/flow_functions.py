import pandas as pd
from sklearn.model_selection import train_test_split


def join(df1, df2, **join_kwargs):
    return pd.merge(df1, df2, **join_kwargs)


def split_train_or_test(train_or_test, train_size, state):
    index = 0 if train_or_test == "train" else 1
    func_name = "train_set" if train_or_test == "train" else "test_set"

    def split(dataframe):
        return train_test_split(dataframe, train_size=train_size,
                                random_state=state)[index]

    split.func_name = func_name

    return split


def model_prediction(model, testset, target=None, columns=None):
    df = testset if target is None else testset.drop(target, axis=1)
    X = df if columns is None else df[columns]
    y_pred = model.predict(X)
    col_name = "prediction" if target is None else target + "_pred"
    return pd.DataFrame(y_pred.reshape((-1, 1)), columns=[col_name])


def model_transformation(model, testset, columns=None):
    X = testset if columns is None else testset[columns]
    X_transformed = pd.DataFrame(model.transform(X),
                                 columns=X.columns,
                                 index=X.index)
    df = testset.copy()
    for column in columns:
        df.loc[:, column] = X_transformed.loc[:, column]
    return df


def select_columns(columns):
    def select(dataframe):
        return dataframe[columns]

    return select
